// Package core is the title storage's client of the account core: the friend list (read rule for
// friends-only rows and friend scopes) and the interaction check (write rule for another
// player's slots). Title storage never stores a friendship, and it announces nothing on the
// core's event stream: no consumer needs a storage write as an event (see docs/protocol.md).
package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	accountv1 "github.com/openpak/account-api/openpak/account/v1"
)

type Client struct {
	social accountv1.SocialClient
	key    string
	// FriendsTTL is how long a friend list is reused (0 = never cached). New sets 2 minutes: a
	// friend added or removed shows in reads within that.
	FriendsTTL time.Duration

	mu      sync.Mutex
	friends map[string]cachedFriends
}

type cachedFriends struct {
	ids []string
	at  time.Time
}

// maxCached bounds the friend cache; past it the cache starts over.
const maxCached = 20000

// Dial opens the core connection. Insecure on purpose: the internal network is the boundary.
func Dial(addr, internalKey string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("title-storage: core dial: %w", err)
	}
	return New(accountv1.NewSocialClient(conn), internalKey), nil
}

// New builds a client on an existing stub; tests hand it a bufconn-backed one.
func New(social accountv1.SocialClient, internalKey string) *Client {
	return &Client{social: social, key: internalKey, FriendsTTL: 2 * time.Minute, friends: map[string]cachedFriends{}}
}

func (c *Client) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	return metadata.AppendToOutgoingContext(ctx, "x-internal-key", c.key), cancel
}

// Friends lists the account's friends (account ids) from the core's graph, cached for FriendsTTL.
// Callers must not modify the slice.
func (c *Client) Friends(ctx context.Context, account string) ([]string, error) {
	if c.FriendsTTL > 0 {
		c.mu.Lock()
		e, ok := c.friends[account]
		c.mu.Unlock()
		if ok && time.Since(e.at) < c.FriendsTTL {
			return e.ids, nil
		}
	}
	out, err := c.listFriends(ctx, account)
	if err != nil || c.FriendsTTL <= 0 {
		return out, err
	}
	c.mu.Lock()
	if len(c.friends) >= maxCached {
		c.friends = map[string]cachedFriends{}
	}
	c.friends[account] = cachedFriends{ids: out, at: time.Now()}
	c.mu.Unlock()
	return out, nil
}

func (c *Client) listFriends(ctx context.Context, account string) ([]string, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	res, err := c.social.ListFriends(ctx, &accountv1.ListFriendsRequest{AccountId: account})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(res.GetFriends()))
	for _, f := range res.GetFriends() {
		out = append(out, f.GetAccountId())
	}
	return out, nil
}

// CanInteract asks whether actor may touch target's data (no block either way). The kind is
// DISCOVER, the lightest, as every console adapter asks it.
func (c *Client) CanInteract(ctx context.Context, actor, target string) (bool, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	r, err := c.social.CheckInteraction(ctx, &accountv1.CheckInteractionRequest{ActorId: actor, TargetId: target, Kind: accountv1.InteractionKind_INTERACTION_KIND_DISCOVER})
	if err != nil {
		return false, err
	}
	return r.GetAllowed(), nil
}
