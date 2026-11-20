package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/m5tshift/slopay/services/invoice-service/internal/invoice"
)

// Server implements StrictServerInterface on top of the invoice service.
type Server struct {
	svc        *invoice.Service
	retryAfter time.Duration
	log        *slog.Logger
}

var _ StrictServerInterface = (*Server)(nil)

// Error codes of the API.
const (
	codeValidation      = "validation_failed"
	codeUnauthorized    = "unauthorized"
	codeNotFound        = "not_found"
	codeXpubNotSet      = "xpub_not_set"
	codeKeyReuse        = "idempotency_key_reuse"
	codeRateUnavailable = "rate_unavailable"
	codeInternal        = "internal"
)

// defaultLimit is the page size when limit is absent (the contract default).
const defaultLimit = 20

var bearer = "Bearer"

func unauthorized() UnauthorizedJSONResponse {
	return UnauthorizedJSONResponse{
		Body:    Error{Code: codeUnauthorized, Message: "Invalid credentials"},
		Headers: UnauthorizedResponseHeaders{WWWAuthenticate: &bearer},
	}
}

func notFound() NotFoundJSONResponse {
	return NotFoundJSONResponse{Code: codeNotFound, Message: "Invoice not found"}
}

type fieldError = struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func validationResponse(details []fieldError) ValidationErrorJSONResponse {
	return ValidationErrorJSONResponse{Code: codeValidation, Message: "Request validation failed", Details: &details}
}

func validation(err error) (ValidationErrorJSONResponse, bool) {
	var ve *invoice.ValidationError
	if !errors.As(err, &ve) {
		return ValidationErrorJSONResponse{}, false
	}
	return validationResponse([]fieldError{{Field: ve.Field, Message: ve.Message}}), true
}

func assetBody(a invoice.Asset) (Asset, error) {
	var out Asset
	var err error
	switch a.Kind {
	case invoice.KindETH:
		err = out.FromAsset0(Asset0{Kind: Asset0KindETH, Decimals: N18})
	case invoice.KindERC20:
		err = out.FromAsset1(Asset1{Kind: ERC20, Contract: a.Contract, Decimals: a.Decimals})
	default:
		err = fmt.Errorf("api: unknown asset kind %q", a.Kind)
	}
	return out, err
}

func invoiceBody(inv invoice.Invoice) (Invoice, error) {
	asset, err := assetBody(inv.Asset)
	if err != nil {
		return Invoice{}, err
	}
	return Invoice{
		InvoiceId: inv.ID, MerchantId: inv.MerchantID, Status: InvoiceStatus(inv.Status), Address: inv.Address,
		Amount: inv.Amount.String(), Asset: asset, OrderId: inv.OrderID, AmountUsdCents: inv.AmountUSDCents,
		Rate: inv.Rate, CreatedAt: inv.CreatedAt, ExpiresAt: inv.ExpiresAt, Sequence: inv.Sequence,
		Confirmations: inv.Confirmations, ReceivedAmount: inv.Received.String(), Overpaid: inv.Overpaid,
	}, nil
}

func pageBody(p invoice.Page) (PaymentPage, error) {
	asset, err := assetBody(p.Asset)
	if err != nil {
		return PaymentPage{}, err
	}
	return PaymentPage{
		InvoiceId: p.InvoiceID, Status: InvoiceStatus(p.Status), Address: p.Address, Amount: p.Amount, Asset: asset,
		OrderId: p.OrderID, ExpiresAt: p.ExpiresAt,
	}, nil
}

// retrySeconds rounds a pause up to whole seconds, at least 1.
func retrySeconds(d time.Duration) int {
	s := int(math.Ceil(d.Seconds()))
	return max(s, 1)
}

// CreateInvoice implements UC-01 and FR-01.
func (s *Server) CreateInvoice(ctx context.Context, req CreateInvoiceRequestObject) (CreateInvoiceResponseObject, error) {
	mid, ok := merchantID(ctx)
	if !ok {
		return CreateInvoice401JSONResponse{unauthorized()}, nil
	}
	r := invoice.CreateRequest{AmountUSDCents: req.Body.AmountUsdCents, OrderID: req.Body.OrderId}
	if req.Body.Asset != nil {
		r.Asset = string(*req.Body.Asset)
	}
	inv, created, err := s.svc.Create(ctx, mid, req.Params.IdempotencyKey, r)
	var rateErr *invoice.RateError
	switch {
	case err == nil:
	case errors.Is(err, invoice.ErrKeyReuse):
		return CreateInvoice422JSONResponse{Code: codeKeyReuse,
			Message: "Idempotency-Key was used with a different request body"}, nil
	case errors.Is(err, invoice.ErrNoXpub):
		return CreateInvoice409JSONResponse{Code: codeXpubNotSet, Message: "Upload the xpub before creating invoices"}, nil
	case errors.As(err, &rateErr):
		s.log.WarnContext(ctx, "invoice not created: no usable rate", "reason", rateErr.Reason, "error", rateErr.Err)
		retry := rateErr.RetryAfter
		if retry <= 0 {
			retry = s.retryAfter
		}
		reason := ErrorReason(rateErr.Reason)
		return CreateInvoice503JSONResponse{RateUnavailableJSONResponse{
			Body:    Error{Code: codeRateUnavailable, Message: "Exchange rate is unavailable", Reason: &reason},
			Headers: RateUnavailableResponseHeaders{RetryAfter: retrySeconds(retry)},
		}}, nil
	default:
		if v, ok := validation(err); ok {
			return CreateInvoice400JSONResponse{v}, nil
		}
		return nil, err
	}
	body, err := invoiceBody(inv)
	if err != nil {
		return nil, err
	}
	if !created {
		return CreateInvoice200JSONResponse(body), nil
	}
	location := "/api/v1/invoices/" + inv.ID.String()
	return CreateInvoice201JSONResponse{Body: body, Headers: CreateInvoice201ResponseHeaders{Location: &location}}, nil
}

// ListInvoices implements the merchant invoice list with keyset pagination.
func (s *Server) ListInvoices(ctx context.Context, req ListInvoicesRequestObject) (ListInvoicesResponseObject, error) {
	mid, ok := merchantID(ctx)
	if !ok {
		return ListInvoices401JSONResponse{unauthorized()}, nil
	}
	p := req.Params
	f := invoice.Filter{Limit: defaultLimit}
	if p.Limit != nil {
		f.Limit = *p.Limit
	}
	if p.Status != nil {
		for _, st := range *p.Status {
			f.Statuses = append(f.Statuses, invoice.Status(st))
		}
	}
	if p.OrderId != nil {
		f.OrderID = *p.OrderId
	}
	if p.CreatedFrom != nil {
		f.CreatedFrom = *p.CreatedFrom
	}
	if p.CreatedTo != nil {
		f.CreatedTo = *p.CreatedTo
	}
	cursor := ""
	if p.Cursor != nil {
		cursor = *p.Cursor
	}
	items, next, err := s.svc.List(ctx, mid, f, cursor)
	if err != nil {
		if v, ok := validation(err); ok {
			return ListInvoices400JSONResponse{v}, nil
		}
		return nil, err
	}
	page := InvoicePage{Items: make([]Invoice, 0, len(items))}
	for _, inv := range items {
		b, err := invoiceBody(inv)
		if err != nil {
			return nil, err
		}
		page.Items = append(page.Items, b)
	}
	if next != "" {
		page.NextCursor = &next
	}
	return ListInvoices200JSONResponse(page), nil
}

// GetInvoice returns an invoice of the merchant; someone else's invoice is 404.
func (s *Server) GetInvoice(ctx context.Context, req GetInvoiceRequestObject) (GetInvoiceResponseObject, error) {
	mid, ok := merchantID(ctx)
	if !ok {
		return GetInvoice401JSONResponse{unauthorized()}, nil
	}
	inv, err := s.svc.Get(ctx, mid, req.Id)
	if errors.Is(err, invoice.ErrNotFound) {
		return GetInvoice404JSONResponse{notFound()}, nil
	}
	if err != nil {
		return nil, err
	}
	body, err := invoiceBody(inv)
	if err != nil {
		return nil, err
	}
	return GetInvoice200JSONResponse(body), nil
}

// GetPaymentPage is the public payment page data (FR-03), read from PostgreSQL.
func (s *Server) GetPaymentPage(ctx context.Context, req GetPaymentPageRequestObject) (GetPaymentPageResponseObject, error) {
	p, err := s.svc.PaymentPage(ctx, req.Id)
	if errors.Is(err, invoice.ErrNotFound) {
		return GetPaymentPage404JSONResponse{notFound()}, nil
	}
	if err != nil {
		return nil, err
	}
	body, err := pageBody(p)
	if err != nil {
		return nil, err
	}
	return GetPaymentPage200JSONResponse(body), nil
}

// GetPaymentStatus is the payment page data for polling, served from the status cache.
func (s *Server) GetPaymentStatus(ctx context.Context, req GetPaymentStatusRequestObject) (GetPaymentStatusResponseObject, error) {
	p, err := s.svc.PaymentStatus(ctx, req.Id)
	if errors.Is(err, invoice.ErrNotFound) {
		return GetPaymentStatus404JSONResponse{notFound()}, nil
	}
	if err != nil {
		return nil, err
	}
	body, err := pageBody(p)
	if err != nil {
		return nil, err
	}
	return GetPaymentStatus200JSONResponse(body), nil
}
