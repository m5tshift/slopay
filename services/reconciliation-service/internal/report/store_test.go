package report

import (
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/m5tshift/slopay/services/reconciliation-service/internal/recon"
)

func TestDocRoundTrip(t *testing.T) {
	at := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	r := recon.Report{RunID: "run", ChainID: 31337, BlockNumber: 93, BlockTime: at, StartedAt: at, FinishedAt: at.Add(time.Second),
		AddressesChecked: 2, Mismatches: 1, AfterWindow: 1, Rows: []recon.Row{
			{Kind: recon.KindInvoicesVsLedger, Address: "0xaa", MerchantID: "m", InvoiceIDs: []string{"i1"},
				Asset:        recon.Asset{Kind: recon.KindETH, Decimals: 18},
				ChainBalance: big.NewInt(10), InvoicesTotal: big.NewInt(10), LedgerTotal: big.NewInt(4), TrackUntil: at},
			{Kind: recon.KindAfterWindow, Address: "0xbb", MerchantID: "m", InvoiceIDs: []string{"i2"},
				Asset:        recon.Asset{Kind: recon.KindERC20, Contract: "0xcc", Decimals: 6},
				ChainBalance: big.NewInt(9), InvoicesTotal: big.NewInt(5), LedgerTotal: big.NewInt(5), Excess: big.NewInt(4), TrackUntil: at},
		}}
	d := toDoc(r, time.Hour)
	if d.Result != "MISMATCH" || !d.ExpireAt.Equal(at.Add(time.Hour)) || d.Published || d.Rows[0].Excess != "" {
		t.Fatalf("doc %+v", d)
	}
	back, err := fromDoc(d)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back, r) {
		t.Fatalf("round trip:\n%+v\n%+v", back, r)
	}
	d.Rows[0].LedgerTotal = "x"
	if _, err := fromDoc(d); err == nil {
		t.Fatal("invalid amount: want error")
	}
}
