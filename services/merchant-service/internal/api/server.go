package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/m5tshift/weigate/services/merchant-service/internal/jwt"
	"github.com/m5tshift/weigate/services/merchant-service/internal/merchant"
)

// Server implements StrictServerInterface on top of the merchant service and the JWT issuer.
type Server struct {
	svc        *merchant.Service
	issuer     *jwt.Issuer
	signer     jwt.Signer
	jwksMaxAge time.Duration
}

var _ StrictServerInterface = (*Server)(nil)

// Error codes of the API.
const (
	codeValidation   = "validation_failed"
	codeUnauthorized = "unauthorized"
	codeTooManyKeys  = "too_many_keys"
	codeKeyNotFound  = "not_found"
	codeLastKey      = "last_key"
	codeInternal     = "internal"
	codeNotDone      = "not_implemented"
)

var bearer = "Bearer"

func unauthorized() UnauthorizedJSONResponse {
	return UnauthorizedJSONResponse{
		Body:    Error{Code: codeUnauthorized, Message: "Invalid credentials"},
		Headers: UnauthorizedResponseHeaders{WWWAuthenticate: &bearer},
	}
}

func validation(err error) (ValidationErrorJSONResponse, bool) {
	var ve *merchant.ValidationError
	if !errors.As(err, &ve) {
		return ValidationErrorJSONResponse{}, false
	}
	return validationResponse([]fieldError{{Field: ve.Field, Message: ve.Message}}), true
}

type fieldError = struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func validationResponse(details []fieldError) ValidationErrorJSONResponse {
	return ValidationErrorJSONResponse{Code: codeValidation, Message: "Request validation failed", Details: &details}
}

type merchantKey struct{}

func withMerchant(ctx context.Context, id uuid.UUID) context.Context {
	return context.WithValue(ctx, merchantKey{}, id)
}

// merchantID is the merchant authenticated by the JWT; the gate middleware guarantees it for protected operations.
func merchantID(ctx context.Context) (uuid.UUID, bool) {
	id, ok := ctx.Value(merchantKey{}).(uuid.UUID)
	return id, ok
}

func apiKey(k merchant.APIKey) ApiKey {
	return ApiKey{Id: k.ID, Status: ApiKeyStatus(k.Status()), CreatedAt: k.CreatedAt, ExpiresAt: k.ExpiresAt}
}

func issuedKey(k merchant.IssuedKey) IssuedApiKey {
	return IssuedApiKey{Id: k.ID, Key: k.Key, Status: IssuedApiKeyStatus(k.Status()), CreatedAt: k.CreatedAt, ExpiresAt: k.ExpiresAt}
}

func merchantBody(m merchant.Merchant) Merchant {
	return Merchant{
		MerchantId: m.ID, Name: m.Name, Email: openapi_types.Email(m.Email), Xpub: m.Xpub, WebhookUrl: m.WebhookURL,
		ToleranceBps: m.ToleranceBps, WebhookSecretVersion: m.WebhookSecretVersion,
	}
}

// RegisterMerchant implements UC-06: the merchant and its first API key, shown once.
func (s *Server) RegisterMerchant(ctx context.Context, req RegisterMerchantRequestObject) (RegisterMerchantResponseObject, error) {
	m, key, err := s.svc.Register(ctx, req.Body.Name, string(req.Body.Email))
	if v, ok := validation(err); ok {
		return RegisterMerchant400JSONResponse{v}, nil
	}
	if err != nil {
		return nil, err
	}
	return RegisterMerchant201JSONResponse{MerchantId: m.ID, ApiKey: issuedKey(key)}, nil
}

// IssueToken exchanges an API key for a JWT.
func (s *Server) IssueToken(ctx context.Context, req IssueTokenRequestObject) (IssueTokenResponseObject, error) {
	id, err := s.svc.Authenticate(ctx, req.Body.ApiKey)
	if errors.Is(err, merchant.ErrBadAPIKey) {
		return IssueToken401JSONResponse{unauthorized()}, nil
	}
	if err != nil {
		return nil, err
	}
	token, err := s.issuer.Issue(ctx, id.String())
	if err != nil {
		return nil, err
	}
	return IssueToken200JSONResponse{
		AccessToken: token,
		ExpiresIn:   TokenResponseExpiresIn(s.issuer.TTL() / time.Second),
		TokenType:   Bearer,
	}, nil
}

// GetJwks returns the public keys the JWTs are verified with.
func (s *Server) GetJwks(ctx context.Context, _ GetJwksRequestObject) (GetJwksResponseObject, error) {
	pubs, err := s.signer.PublicKeys(ctx)
	if err != nil {
		return nil, err
	}
	keys := make([]Jwk, 0, len(pubs))
	for _, p := range pubs {
		k, err := jwt.ToJWK(p)
		if err != nil {
			return nil, err
		}
		keys = append(keys, Jwk{
			Kty: JwkKty(k.Kty), Crv: JwkCrv(k.Crv), Alg: JwkAlg(k.Alg), Use: JwkUse(k.Use), Kid: k.Kid, X: k.X, Y: k.Y,
		})
	}
	cc := fmt.Sprintf("public, max-age=%d", int(s.jwksMaxAge/time.Second))
	return GetJwks200JSONResponse{Body: Jwks{Keys: keys}, Headers: GetJwks200ResponseHeaders{CacheControl: &cc}}, nil
}

// GetMerchant returns the settings of the authenticated merchant.
func (s *Server) GetMerchant(ctx context.Context, _ GetMerchantRequestObject) (GetMerchantResponseObject, error) {
	id, _ := merchantID(ctx)
	m, err := s.svc.Get(ctx, id)
	if errors.Is(err, merchant.ErrNotFound) {
		return GetMerchant401JSONResponse{unauthorized()}, nil
	}
	if err != nil {
		return nil, err
	}
	return GetMerchant200JSONResponse(merchantBody(m)), nil
}

// SetXpub implements UC-07.
func (s *Server) SetXpub(ctx context.Context, req SetXpubRequestObject) (SetXpubResponseObject, error) {
	id, _ := merchantID(ctx)
	m, first, err := s.svc.SetXpub(ctx, id, req.Body.Xpub)
	if v, ok := validation(err); ok {
		return SetXpub400JSONResponse{v}, nil
	}
	if errors.Is(err, merchant.ErrNotFound) {
		return SetXpub401JSONResponse{unauthorized()}, nil
	}
	if err != nil {
		return nil, err
	}
	return SetXpub200JSONResponse{Xpub: *m.Xpub, FirstAddress: first}, nil
}

// SetWebhookSettings sets the webhook URL and the tolerance.
func (s *Server) SetWebhookSettings(ctx context.Context, req SetWebhookSettingsRequestObject) (SetWebhookSettingsResponseObject, error) {
	id, _ := merchantID(ctx)
	m, err := s.svc.SetWebhook(ctx, id, req.Body.WebhookUrl, req.Body.ToleranceBps)
	if v, ok := validation(err); ok {
		return SetWebhookSettings400JSONResponse{v}, nil
	}
	if errors.Is(err, merchant.ErrNotFound) {
		return SetWebhookSettings401JSONResponse{unauthorized()}, nil
	}
	if err != nil {
		return nil, err
	}
	return SetWebhookSettings200JSONResponse(merchantBody(m)), nil
}

// ListApiKeys lists the active keys, including the expiring ones.
func (s *Server) ListApiKeys(ctx context.Context, _ ListApiKeysRequestObject) (ListApiKeysResponseObject, error) {
	id, _ := merchantID(ctx)
	keys, err := s.svc.ListKeys(ctx, id)
	if err != nil {
		return nil, err
	}
	items := make([]ApiKey, 0, len(keys))
	for _, k := range keys {
		items = append(items, apiKey(k))
	}
	return ListApiKeys200JSONResponse{Items: items}, nil
}

// IssueApiKey issues a new key for a planned rotation.
func (s *Server) IssueApiKey(ctx context.Context, _ IssueApiKeyRequestObject) (IssueApiKeyResponseObject, error) {
	id, _ := merchantID(ctx)
	key, err := s.svc.IssueKey(ctx, id)
	switch {
	case errors.Is(err, merchant.ErrTooManyKeys):
		return IssueApiKey409JSONResponse{Code: codeTooManyKeys, Message: "The limit of active API keys is reached; revoke a key first"}, nil
	case errors.Is(err, merchant.ErrNotFound):
		return IssueApiKey401JSONResponse{unauthorized()}, nil
	case err != nil:
		return nil, err
	}
	return IssueApiKey201JSONResponse(issuedKey(key)), nil
}

// RevokeApiKey revokes a key at once.
func (s *Server) RevokeApiKey(ctx context.Context, req RevokeApiKeyRequestObject) (RevokeApiKeyResponseObject, error) {
	id, _ := merchantID(ctx)
	err := s.svc.RevokeKey(ctx, id, req.KeyId)
	switch {
	case errors.Is(err, merchant.ErrKeyNotFound):
		return RevokeApiKey404JSONResponse{Code: codeKeyNotFound, Message: "API key not found"}, nil
	case errors.Is(err, merchant.ErrLastKey):
		return RevokeApiKey409JSONResponse{Code: codeLastKey, Message: "The last active API key cannot be revoked"}, nil
	case errors.Is(err, merchant.ErrNotFound):
		return RevokeApiKey401JSONResponse{unauthorized()}, nil
	case err != nil:
		return nil, err
	}
	return RevokeApiKey204Response{}, nil
}

// notImplementedResponse is 501: no secret store (OpenBao) is configured.
type notImplementedResponse struct{}

func (notImplementedResponse) VisitRotateWebhookSecretResponse(w http.ResponseWriter) error {
	return writeJSON(w, http.StatusNotImplemented, Error{Code: codeNotDone, Message: "Webhook secret rotation is not configured"})
}

// RotateWebhookSecret writes a new secret version to OpenBao and publishes the new version. The secret is
// shown once.
func (s *Server) RotateWebhookSecret(ctx context.Context, _ RotateWebhookSecretRequestObject) (RotateWebhookSecretResponseObject, error) {
	id, _ := merchantID(ctx)
	rs, err := s.svc.RotateWebhookSecret(ctx, id)
	switch {
	case errors.Is(err, merchant.ErrRotationUnavailable):
		return notImplementedResponse{}, nil
	case errors.Is(err, merchant.ErrNotFound):
		return RotateWebhookSecret401JSONResponse{unauthorized()}, nil
	case err != nil:
		return nil, err
	}
	return RotateWebhookSecret200JSONResponse{Version: rs.Version, Secret: rs.Secret}, nil
}
