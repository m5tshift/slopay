package tracked

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
)

const key = "chain-watcher:31337:tracked"

func TestKey(t *testing.T) {
	if Key(31337) != key {
		t.Fatal(Key(31337))
	}
}

func TestWithoutClient(t *testing.T) {
	v := NewValkey(31337, time.Second, nil)
	if _, err := v.Members(ctx, "t", []string{"0xa"}); !errors.Is(err, ErrNoClient) {
		t.Errorf("members: %v", err)
	}
	if err := v.Load(ctx, "t", func(func([]string) error) error { return nil }); !errors.Is(err, ErrNoClient) {
		t.Errorf("load: %v", err)
	}
	if err := v.Add(ctx, []string{"0xa"}); !errors.Is(err, ErrNoClient) {
		t.Errorf("add: %v", err)
	}
	if err := v.Remove(ctx, nil); err != nil {
		t.Errorf("remove nothing: %v", err)
	}
	v.Close()
}

func TestMembers(t *testing.T) {
	client := mock.NewClient(gomock.NewController(t))
	v := NewValkey(31337, time.Second, quiet)
	v.Attach(client)

	client.EXPECT().DoMulti(gomock.Any(), mock.Match("SISMEMBER", key, "token:t"), mock.Match("SISMEMBER", key, "0xa"),
		mock.Match("SISMEMBER", key, "0xb")).
		Return([]valkey.ValkeyResult{mock.Result(mock.ValkeyInt64(1)), mock.Result(mock.ValkeyInt64(0)), mock.Result(mock.ValkeyInt64(1))})
	m, err := v.Members(ctx, "t", []string{"0xa", "0xb"})
	if err != nil || len(m) != 1 || !m["0xb"] {
		t.Fatalf("members %v %v", m, err)
	}

	client.EXPECT().DoMulti(gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]valkey.ValkeyResult{mock.Result(mock.ValkeyInt64(0)), mock.Result(mock.ValkeyInt64(1))})
	if _, err := v.Members(ctx, "t", []string{"0xa"}); !errors.Is(err, ErrStale) {
		t.Fatalf("stale: %v", err)
	}
	client.EXPECT().DoMulti(gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]valkey.ValkeyResult{mock.Result(mock.ValkeyInt64(1)), mock.ErrorResult(errors.New("down"))})
	if _, err := v.Members(ctx, "t", []string{"0xa"}); err == nil || errors.Is(err, ErrStale) {
		t.Fatalf("error: %v", err)
	}
	client.EXPECT().Close()
	v.Close()
}

func TestLoadAddRemove(t *testing.T) {
	client := mock.NewClient(gomock.NewController(t))
	v := NewValkey(31337, time.Second, quiet)
	v.Attach(client)
	ok := mock.Result(mock.ValkeyInt64(1))

	gomock.InOrder(
		client.EXPECT().Do(gomock.Any(), mock.Match("DEL", key)).Return(ok),
		client.EXPECT().Do(gomock.Any(), mock.Match("SADD", key, "0xa", "0xb")).Return(ok),
		client.EXPECT().Do(gomock.Any(), mock.Match("SADD", key, "0xc")).Return(ok),
		client.EXPECT().Do(gomock.Any(), mock.Match("SADD", key, "token:t")).Return(ok),
	)
	err := v.Load(ctx, "t", func(add func([]string) error) error {
		if err := add([]string{"0xa", "0xb"}); err != nil {
			return err
		}
		return add([]string{"0xc"})
	})
	if err != nil {
		t.Fatal(err)
	}

	down := mock.ErrorResult(errors.New("down"))
	client.EXPECT().Do(gomock.Any(), mock.Match("DEL", key)).Return(down)
	if err := v.Load(ctx, "t", nil); err == nil {
		t.Fatal("del: no error")
	}
	// An interrupted load does not add the token.
	client.EXPECT().Do(gomock.Any(), mock.Match("DEL", key)).Return(ok)
	if err := v.Load(ctx, "t", func(func([]string) error) error { return errors.New("mongodb down") }); err == nil {
		t.Fatal("fill: no error")
	}
	client.EXPECT().Do(gomock.Any(), mock.Match("DEL", key)).Return(ok)
	client.EXPECT().Do(gomock.Any(), mock.Match("SADD", key, "token:t")).Return(down)
	if err := v.Load(ctx, "t", func(func([]string) error) error { return nil }); err == nil {
		t.Fatal("token: no error")
	}

	client.EXPECT().Do(gomock.Any(), mock.Match("SADD", key, "0xd")).Return(ok)
	if err := v.Add(ctx, []string{"0xd"}); err != nil {
		t.Fatal(err)
	}
	client.EXPECT().Do(gomock.Any(), mock.Match("SREM", key, "0xa", "0xd")).Return(down)
	if err := v.Remove(ctx, []string{"0xa", "0xd"}); err == nil {
		t.Fatal("remove: no error")
	}
	if err := v.Add(ctx, nil); err != nil {
		t.Fatal(err)
	}
}

func TestConnect(t *testing.T) {
	client := mock.NewClient(gomock.NewController(t))
	v := NewValkey(31337, time.Second, quiet)
	calls := 0
	v.Connect(ctx, func(context.Context) (valkey.Client, error) {
		if calls++; calls < 3 {
			return nil, errors.New("down")
		}
		return client, nil
	}, time.Millisecond)
	if v.get() != client || calls != 3 {
		t.Fatalf("calls %d", calls)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	v2 := NewValkey(1, time.Second, quiet)
	v2.Connect(cctx, func(context.Context) (valkey.Client, error) { return nil, errors.New("down") }, time.Hour)
	if v2.get() != nil {
		t.Fatal("attached")
	}
}
