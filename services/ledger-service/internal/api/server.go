package api

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/m5tshift/weigate/services/ledger-service/internal/ledger"
)

// Server implements StrictServerInterface on top of the ledger service.
type Server struct {
	svc *ledger.Service
}

var _ StrictServerInterface = (*Server)(nil)

func validation(err error) (ValidationErrorJSONResponse, bool) {
	var ve *ledger.ValidationError
	if !errors.As(err, &ve) {
		return ValidationErrorJSONResponse{}, false
	}
	return ValidationErrorJSONResponse(validationError([]string{ve.Field})), true
}

func asset(a ledger.Asset) (Asset, error) {
	raw, err := json.Marshal(a)
	if err != nil {
		return Asset{}, err
	}
	var out Asset
	err = out.UnmarshalJSON(raw)
	return out, err
}

func totals(ts []ledger.Totals) ([]AssetTotals, error) {
	out := make([]AssetTotals, 0, len(ts))
	for _, t := range ts {
		a, err := asset(t.Asset)
		if err != nil {
			return nil, err
		}
		out = append(out, AssetTotals{Asset: a, Received: t.Received.String(), Fee: t.Fee.String(), Postings: int(t.Postings)})
	}
	return out, nil
}

func posting(p ledger.Posting) (Posting, error) {
	a, err := asset(p.Transfer.Asset)
	if err != nil {
		return Posting{}, err
	}
	var logIndex *int
	if p.Transfer.LogIndex != nil {
		v := int(*p.Transfer.LogIndex) //nolint:gosec // log indexes are small
		logIndex = &v
	}
	return Posting{
		PostingId:  p.ID,
		MerchantId: p.MerchantID,
		InvoiceId:  p.InvoiceID,
		Address:    p.Address,
		Transfer: Transfer{
			BlockNumber: int(p.Transfer.BlockNumber), //nolint:gosec // block numbers fit int
			BlockHash:   p.Transfer.BlockHash,
			TxHash:      p.Transfer.TxHash,
			LogIndex:    logIndex,
			Amount:      p.Transfer.Amount.String(),
			Asset:       a,
		},
		DebitAccount:  p.DebitAccount(),
		CreditAccount: p.CreditAccount(),
		Fee:           p.Fee.String(),
		PostedAt:      p.PostedAt,
	}, nil
}

func postings(ps []ledger.Posting) ([]Posting, error) {
	out := make([]Posting, 0, len(ps))
	for _, p := range ps {
		v, err := posting(p)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func next(p ledger.Page) *string {
	if p.Next == "" {
		return nil
	}
	return &p.Next
}

// GetMerchantStatement returns the postings of the merchant for the period and the totals per asset.
func (s *Server) GetMerchantStatement(ctx context.Context, req GetMerchantStatementRequestObject) (GetMerchantStatementResponseObject, error) {
	st, err := s.svc.MerchantStatement(ctx, req.MerchantId, req.Params.From, req.Params.To, req.Params.Limit, req.Params.Cursor)
	if v, ok := validation(err); ok {
		return GetMerchantStatement400JSONResponse{v}, nil
	}
	if errors.Is(err, ledger.ErrNotFound) {
		return GetMerchantStatement404JSONResponse{Code: codeNotFound, Message: "Merchant not found"}, nil
	}
	if err != nil {
		return nil, err
	}
	ts, err := totals(st.Totals)
	if err != nil {
		return nil, err
	}
	items, err := postings(st.Items)
	if err != nil {
		return nil, err
	}
	return GetMerchantStatement200JSONResponse{
		MerchantId: st.MerchantID, From: st.From, To: st.To, Totals: ts, Items: items, NextCursor: next(st.Page),
	}, nil
}

// GetAddressesStatement returns the totals of each requested address.
func (s *Server) GetAddressesStatement(ctx context.Context, req GetAddressesStatementRequestObject) (GetAddressesStatementResponseObject, error) {
	st, err := s.svc.AddressesStatement(ctx, req.Params.Address)
	if v, ok := validation(err); ok {
		return GetAddressesStatement400JSONResponse{v}, nil
	}
	if err != nil {
		return nil, err
	}
	items := make([]AddressTotals, 0, len(st))
	for _, a := range st {
		ts, err := totals(a.Totals)
		if err != nil {
			return nil, err
		}
		items = append(items, AddressTotals{Address: a.Address, Totals: ts})
	}
	return GetAddressesStatement200JSONResponse{Items: items}, nil
}

// ListAddressPostings returns a page of the postings of one address.
func (s *Server) ListAddressPostings(ctx context.Context, req ListAddressPostingsRequestObject) (ListAddressPostingsResponseObject, error) {
	page, err := s.svc.AddressPostings(ctx, req.Address, req.Params.Limit, req.Params.Cursor)
	if v, ok := validation(err); ok {
		return ListAddressPostings400JSONResponse{v}, nil
	}
	if err != nil {
		return nil, err
	}
	items, err := postings(page.Items)
	if err != nil {
		return nil, err
	}
	return ListAddressPostings200JSONResponse{Address: req.Address, Items: items, NextCursor: next(page)}, nil
}
