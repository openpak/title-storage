// Package grpcapi is the title storage's gRPC surface for the console-side adapters (np-tus,
// np-uploader, later wfc-sake, nex, xb-stfd, npln): the same rules as the service, one wire.
package grpcapi

import (
	"context"
	"errors"
	"log"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/openpak/title-storage/internal/service"
	"github.com/openpak/title-storage/internal/store"
	pb "github.com/openpak/title-storage/proto/openpak/titlestorage/v1"
)

type Server struct {
	pb.UnimplementedTitleStorageServer
	svc *service.Service
	key string
}

func New(svc *service.Service, internalKey string) *Server {
	return &Server{svc: svc, key: internalKey}
}

func (s *Server) authorize(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || s.key == "" {
		return status.Error(codes.Unauthenticated, "missing internal credentials")
	}
	if vals := md.Get("x-internal-key"); len(vals) == 0 || vals[0] != s.key {
		return status.Error(codes.Unauthenticated, "missing internal credentials")
	}
	return nil
}

func ref(r *pb.TitleRef) service.Ref {
	return service.Ref{Network: r.GetNetwork(), Title: r.GetTitle()}
}

func visIn(v pb.Visibility) string {
	switch v {
	case pb.Visibility_VISIBILITY_PRIVATE:
		return store.Private
	case pb.Visibility_VISIBILITY_FRIENDS:
		return store.Friends
	case pb.Visibility_VISIBILITY_PUBLIC:
		return store.Public
	case pb.Visibility_VISIBILITY_RECIPIENTS:
		return store.Recipients
	}
	return ""
}

func visOut(v string) pb.Visibility {
	switch v {
	case store.Private:
		return pb.Visibility_VISIBILITY_PRIVATE
	case store.Friends:
		return pb.Visibility_VISIBILITY_FRIENDS
	case store.Public:
		return pb.Visibility_VISIBILITY_PUBLIC
	case store.Recipients:
		return pb.Visibility_VISIBILITY_RECIPIENTS
	}
	return pb.Visibility_VISIBILITY_UNSPECIFIED
}

func cond(c *pb.Condition) service.Condition {
	var out service.Condition
	if c != nil && c.ExpectUpdatedUnixMs != nil {
		t := time.UnixMilli(c.GetExpectUpdatedUnixMs()).UTC()
		out.Updated = &t
	}
	if c != nil && c.ExpectSetter != nil {
		v := c.GetExpectSetter()
		out.Setter = &v
	}
	return out
}

func pbVar(v store.Variable) *pb.Variable {
	return &pb.Variable{Owner: v.Owner, Slot: v.Slot, Value: v.Value, Setter: v.Setter, UpdatedUnixMs: v.Updated.UnixMilli(),
		Visibility: visOut(v.Visibility), Network: v.Network}
}

func pbVarResult(r service.VarResult) *pb.VariableResult {
	out := &pb.VariableResult{Stored: r.Stored, ConditionFailed: r.CondFail, HadPrevious: r.HadPrev, PreviousValue: r.Previous}
	if r.Exists {
		out.Variable = pbVar(r.Var)
	}
	return out
}

func pbObj(o store.Object) *pb.Object {
	out := &pb.Object{Id: uint64(o.ID), Title: o.Title, Owner: o.Owner, Collection: o.Collection, Name: o.Name, Info: o.Info, MetaJson: o.Meta,
		Body: o.Body, Size: uint64(o.Size), Setter: o.Setter, Visibility: visOut(o.Visibility), Network: o.Network,
		CreatedUnixMs: o.Created.UnixMilli(), UpdatedUnixMs: o.Updated.UnixMilli(), GetUrl: o.GetURL, Blob: o.BlobKey != "", Sha256: o.SHA256,
		Recipients: o.Recipients, RecipientsIncludeFriends: o.RecipientsFriends}
	if o.Expires != nil {
		out.ExpiresUnixMs = o.Expires.UnixMilli()
	}
	for _, r := range o.Ratings {
		out.Ratings = append(out.Ratings, pbRating(r, time.Time{}))
	}
	return out
}

func policyIn(p *pb.RatingPolicy) store.RatingPolicy {
	out := store.RatingPolicy{Initial: p.GetInitialValue(), Lock: p.GetLock(), IntervalSeconds: int32(min(p.GetIntervalSeconds(), 1<<31-1)),
		PeriodDay: p.GetPeriodDay(), PeriodHour: p.GetPeriodHour(), Modifiable: p.GetModifiable(), RoundMinus: p.GetRoundMinus(), NoSelf: p.GetDisableSelfRating()}
	if p != nil && p.RangeMin != nil {
		v := p.GetRangeMin()
		out.Min = &v
	}
	if p != nil && p.RangeMax != nil {
		v := p.GetRangeMax()
		out.Max = &v
	}
	return out
}

func policyOut(p store.RatingPolicy) *pb.RatingPolicy {
	return &pb.RatingPolicy{InitialValue: p.Initial, Lock: p.Lock, IntervalSeconds: uint32(max(p.IntervalSeconds, 0)), PeriodDay: p.PeriodDay,
		PeriodHour: p.PeriodHour, RangeMin: p.Min, RangeMax: p.Max, Modifiable: p.Modifiable, RoundMinus: p.RoundMinus, DisableSelfRating: p.NoSelf}
}

// pbRating renders a slot; with now set, the rater's lock is worked out at that time.
func pbRating(r store.Rating, now time.Time) *pb.Rating {
	out := &pb.Rating{Id: uint64(r.ID), Slot: r.Slot, Total: r.Total, Count: uint32(r.Count), InitialValue: r.Policy.Initial, Policy: policyOut(r.Policy)}
	if r.Rated {
		out.Rated, out.RaterValue, out.RaterCount, out.RatedUnixMs = true, r.RaterValue, uint32(r.RaterCount), r.RatedAt.UnixMilli()
		if !now.IsZero() {
			switch until, forever := r.Lock(now); {
			case forever:
				out.LockExpiresUnixMs = -1
			case !until.IsZero():
				out.LockExpiresUnixMs = until.UnixMilli()
			}
		}
	}
	return out
}

func (s *Server) InitRating(ctx context.Context, req *pb.InitRatingRequest) (*pb.RatingResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	r, err := s.svc.InitRating(ctx, req.GetNetwork(), req.GetWriter(), int64(req.GetId()), req.GetSlot(), policyIn(req.GetPolicy()))
	if err != nil {
		return nil, mapErr(err)
	}
	if r == nil {
		return &pb.RatingResponse{}, nil
	}
	return &pb.RatingResponse{Rating: pbRating(*r, store.Now()), Found: true}, nil
}

func (s *Server) RateObject(ctx context.Context, req *pb.RateObjectRequest) (*pb.RatingResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	var p *store.RatingPolicy
	if req.GetPolicy() != nil {
		v := policyIn(req.GetPolicy())
		p = &v
	}
	r, found, locked, err := s.svc.RateObject(ctx, req.GetNetwork(), req.GetRater(), int64(req.GetId()), req.GetSlot(), req.GetValue(), p)
	if err != nil {
		return nil, mapErr(err)
	}
	if !found {
		return &pb.RatingResponse{}, nil
	}
	return &pb.RatingResponse{Rating: pbRating(r, store.Now()), Found: true, Locked: locked}, nil
}

func (s *Server) GetRatings(ctx context.Context, req *pb.GetRatingsRequest) (*pb.GetRatingsResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	ids := make([]int64, len(req.GetIds()))
	for i, id := range req.GetIds() {
		ids[i] = int64(id)
	}
	rs, err := s.svc.GetRatings(ctx, req.GetNetwork(), req.GetViewer(), ids, req.GetSlots(), req.GetRater())
	if err != nil {
		return nil, mapErr(err)
	}
	out := &pb.GetRatingsResponse{}
	now := store.Now()
	for _, r := range rs {
		out.Ratings = append(out.Ratings, pbRating(r, now))
	}
	return out, nil
}

func (s *Server) ResetRatings(ctx context.Context, req *pb.ResetRatingsRequest) (*pb.DeleteResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	n, _, err := s.svc.ResetRatings(ctx, req.GetNetwork(), req.GetWriter(), int64(req.GetId()), req.GetSlots())
	if err != nil {
		return nil, mapErr(err)
	}
	return &pb.DeleteResponse{Deleted: uint32(n)}, nil
}

func objResp(o *store.Object) *pb.ObjectResponse {
	if o == nil {
		return &pb.ObjectResponse{}
	}
	return &pb.ObjectResponse{Object: pbObj(*o), Found: true}
}

func (s *Server) SetVariables(ctx context.Context, req *pb.SetVariablesRequest) (*pb.SetVariablesResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	items := make(map[int32]int64, len(req.GetItems()))
	for _, it := range req.GetItems() {
		items[it.GetSlot()] = it.GetValue()
	}
	err := s.svc.SetVariables(ctx, ref(req.GetRef()), req.GetOwner(), req.GetWriter(), req.GetSetter(), visIn(req.GetVisibility()), items)
	if err != nil {
		return nil, mapErr(err)
	}
	return &pb.SetVariablesResponse{}, nil
}

func (s *Server) ListVariables(ctx context.Context, req *pb.ListVariablesRequest) (*pb.ListVariablesResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	vs, hits, err := s.svc.ListVariables(ctx, ref(req.GetRef()), service.Query{Viewer: req.GetViewer(), Owners: req.GetOwners(),
		Friends: req.GetFriendsOfViewer(), Self: req.GetIncludeSelf(), Slots: req.GetSlots(), Sort: req.GetSort(),
		Ascending: req.GetAscending(), Offset: int(req.GetOffset()), Limit: int(req.GetLimit())})
	if err != nil {
		return nil, mapErr(err)
	}
	out := &pb.ListVariablesResponse{Hits: uint32(hits)}
	for _, v := range vs {
		out.Variables = append(out.Variables, pbVar(v))
	}
	return out, nil
}

func (s *Server) AddVariable(ctx context.Context, req *pb.AddVariableRequest) (*pb.VariableResult, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	r, err := s.svc.AddVariable(ctx, ref(req.GetRef()), req.GetOwner(), req.GetWriter(), req.GetSetter(), visIn(req.GetVisibility()),
		req.GetSlot(), req.GetDelta(), cond(req.GetCondition()))
	if err != nil {
		return nil, mapErr(err)
	}
	return pbVarResult(r), nil
}

func (s *Server) TrySetVariable(ctx context.Context, req *pb.TrySetVariableRequest) (*pb.VariableResult, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	r, err := s.svc.TrySetVariable(ctx, ref(req.GetRef()), req.GetOwner(), req.GetWriter(), req.GetSetter(), visIn(req.GetVisibility()),
		req.GetSlot(), req.GetValue(), req.GetOp(), req.GetOperand(), cond(req.GetCondition()))
	if err != nil {
		return nil, mapErr(err)
	}
	return pbVarResult(r), nil
}

func (s *Server) DeleteVariables(ctx context.Context, req *pb.DeleteVariablesRequest) (*pb.DeleteResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	n, err := s.svc.DeleteVariables(ctx, ref(req.GetRef()), req.GetOwner(), req.GetWriter(), req.GetSlots())
	if err != nil {
		return nil, mapErr(err)
	}
	return &pb.DeleteResponse{Deleted: uint32(n)}, nil
}

func (s *Server) PutObject(ctx context.Context, req *pb.PutObjectRequest) (*pb.ObjectResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	o, failed, err := s.svc.PutObject(ctx, ref(req.GetRef()), service.Put{Owner: req.GetOwner(), Writer: req.GetWriter(), Setter: req.GetSetter(),
		Collection: req.GetCollection(), Name: req.GetName(), Info: req.GetInfo(), Meta: req.GetMetaJson(), Body: req.GetBody(),
		Vis: visIn(req.GetVisibility()), Recipients: req.GetRecipients(), RecipientsFriends: req.GetRecipientsIncludeFriends(),
		TTL: time.Duration(req.GetTtlSeconds()) * time.Second, Cond: cond(req.GetCondition())})
	if err != nil {
		return nil, mapErr(err)
	}
	if failed {
		return &pb.ObjectResponse{ConditionFailed: true}, nil
	}
	return objResp(&o), nil
}

func (s *Server) GetObject(ctx context.Context, req *pb.GetObjectRequest) (*pb.ObjectResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	o, err := s.svc.GetObject(ctx, ref(req.GetRef()), req.GetViewer(), req.GetOwner(), req.GetCollection(), req.GetName(), req.GetIncludeBody())
	if err != nil {
		return nil, mapErr(err)
	}
	return objResp(o), nil
}

func (s *Server) GetObjectByID(ctx context.Context, req *pb.GetObjectByIDRequest) (*pb.ObjectResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	o, err := s.svc.GetObjectByID(ctx, req.GetNetwork(), req.GetViewer(), int64(req.GetId()), req.GetIncludeBody(), req.GetIncludeRatings())
	if err != nil {
		return nil, mapErr(err)
	}
	return objResp(o), nil
}

func (s *Server) ListObjects(ctx context.Context, req *pb.ListObjectsRequest) (*pb.ListObjectsResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	os, hits, err := s.svc.ListObjects(ctx, ref(req.GetRef()), service.ObjQuery{
		Query: service.Query{Viewer: req.GetViewer(), Owners: req.GetOwners(), Friends: req.GetFriendsOfViewer(), Self: req.GetIncludeSelf(),
			Sort: req.GetSort(), Ascending: req.GetAscending(), Offset: int(req.GetOffset()), Limit: int(req.GetLimit())},
		AllOwners: req.GetAllOwners(), ExcludeOwners: req.GetExcludeOwners(), Collection: req.GetCollection(), Collections: req.GetCollections(),
		Names: req.GetNames(), MetaFilter: req.GetMetaFilterJson(), MetaRange: req.GetMetaRangeJson(),
		CreatedAfter: req.GetCreatedAfterMs(), CreatedBefore: req.GetCreatedBeforeMs(), UpdatedAfter: req.GetUpdatedAfterMs(),
		UpdatedBefore: req.GetUpdatedBeforeMs(), MinRatings: req.GetMinRatingCount(), Received: req.GetReceived(), Body: req.GetIncludeBody(), Ratings: req.GetIncludeRatings()})
	if err != nil {
		return nil, mapErr(err)
	}
	out := &pb.ListObjectsResponse{Hits: uint32(hits)}
	for _, o := range os {
		out.Objects = append(out.Objects, pbObj(o))
	}
	return out, nil
}

func (s *Server) DeleteObjects(ctx context.Context, req *pb.DeleteObjectsRequest) (*pb.DeleteResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	n, err := s.svc.DeleteObjects(ctx, ref(req.GetRef()), req.GetOwner(), req.GetWriter(), req.GetCollection(), req.GetNames())
	if err != nil {
		return nil, mapErr(err)
	}
	return &pb.DeleteResponse{Deleted: uint32(n)}, nil
}

func (s *Server) PrepareUpload(ctx context.Context, req *pb.PrepareUploadRequest) (*pb.PrepareUploadResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	if req.GetSize() > 1<<40 {
		return nil, mapErr(service.ErrTooLarge)
	}
	p, err := s.svc.PrepareUpload(ctx, ref(req.GetRef()), service.Prepare{Owner: req.GetOwner(), Writer: req.GetWriter(), Setter: req.GetSetter(),
		Collection: req.GetCollection(), Name: req.GetName(), Size: int64(req.GetSize()), SHA256: req.GetSha256(), Info: req.GetInfo(),
		Meta: req.GetMetaJson(), Vis: visIn(req.GetVisibility()), Recipients: req.GetRecipients(), RecipientsFriends: req.GetRecipientsIncludeFriends(),
		TTL: time.Duration(req.GetTtlSeconds()) * time.Second, PostForm: req.GetPostForm()})
	if err != nil {
		return nil, mapErr(err)
	}
	return &pb.PrepareUploadResponse{Id: uint64(p.ID), Method: p.Method, PutUrl: p.URL, Headers: p.Headers, FormFields: p.FormFields,
		ExpiresUnixMs: p.Expires.UnixMilli()}, nil
}

func (s *Server) CompleteUpload(ctx context.Context, req *pb.CompleteUploadRequest) (*pb.ObjectResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	o, err := s.svc.CompleteUpload(ctx, int64(req.GetId()), req.GetWriter())
	if err != nil {
		return nil, mapErr(err)
	}
	return objResp(o), nil
}

func (s *Server) Usage(ctx context.Context, req *pb.UsageRequest) (*pb.UsageResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	u, err := s.svc.Usage(ctx, ref(req.GetRef()), req.GetOwner())
	if err != nil {
		return nil, mapErr(err)
	}
	return &pb.UsageResponse{Bytes: uint64(u.Bytes), Objects: uint32(u.Objects), Variables: uint32(u.Variables), QuotaBytes: uint64(u.QuotaBytes),
		QuotaObjects: uint32(u.QuotaObjects), MaxObjectBytes: uint32(u.MaxObjectBytes)}, nil
}

func (s *Server) MapTitle(ctx context.Context, req *pb.MapTitleRequest) (*pb.MapTitleResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	if err := s.svc.Store.MapTitle(ctx, req.GetTitle(), req.GetName(), req.GetNetwork(), req.GetNativeId()); err != nil {
		return nil, mapErr(err)
	}
	return &pb.MapTitleResponse{}, nil
}

func pbTitle(t store.Title) *pb.TitleInfo {
	return &pb.TitleInfo{Title: t.Key, Name: t.Name, CrossNetwork: t.Cross, QuotaBytes: uint64(t.QuotaBytes), QuotaObjects: uint32(t.QuotaObjects),
		MaxObjectBytes: uint32(t.MaxObjectBytes), DefaultVisibility: visOut(t.DefaultVis), NativeIds: t.NativeIDs}
}

func (s *Server) UpsertTitle(ctx context.Context, req *pb.UpsertTitleRequest) (*pb.TitleInfo, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	if req.GetDefaultVisibility() == pb.Visibility_VISIBILITY_RECIPIENTS {
		return nil, mapErr(service.ErrBadRequest) // a recipient list is per object
	}
	p := store.TitlePolicy{Name: req.GetName(), QuotaBytes: int64(req.GetQuotaBytes()), QuotaObjects: int(req.GetQuotaObjects()),
		MaxObjectBytes: int(req.GetMaxObjectBytes()), DefaultVis: visIn(req.GetDefaultVisibility())}
	switch req.GetCrossNetwork() {
	case pb.Tristate_TRISTATE_TRUE:
		v := true
		p.Cross = &v
	case pb.Tristate_TRISTATE_FALSE:
		v := false
		p.Cross = &v
	}
	t, err := s.svc.Store.UpsertTitle(ctx, req.GetTitle(), p)
	if err != nil {
		return nil, mapErr(err)
	}
	return pbTitle(t), nil
}

func (s *Server) ListTitles(ctx context.Context, _ *pb.ListTitlesRequest) (*pb.ListTitlesResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	ts, err := s.svc.Store.Titles(ctx)
	if err != nil {
		return nil, mapErr(err)
	}
	out := &pb.ListTitlesResponse{}
	for _, t := range ts {
		out.Titles = append(out.Titles, pbTitle(t))
	}
	return out, nil
}

func mapErr(err error) error {
	switch {
	case errors.Is(err, service.ErrBadAccount), errors.Is(err, service.ErrBadRequest), errors.Is(err, store.ErrOverflow),
		errors.Is(err, service.ErrRange), errors.Is(err, store.ErrNoRatingSlot):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, service.ErrTooLarge), errors.Is(err, store.ErrQuota):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, service.ErrForbidden), errors.Is(err, service.ErrSelfRating):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, service.ErrNeedNet), errors.Is(err, store.ErrConflict), errors.Is(err, service.ErrNoBlobs), errors.Is(err, service.ErrMismatch):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, store.ErrNoTitle), errors.Is(err, service.ErrNoUpload):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, service.ErrBlobs):
		log.Printf("title-storage: %v", err)
		return status.Error(codes.Unavailable, "blob store unavailable")
	}
	// The account core being down is the caller's retry, not a service bug.
	if c := status.Code(err); c == codes.Unavailable || c == codes.DeadlineExceeded {
		return status.Error(codes.Unavailable, "account core unavailable")
	}
	log.Printf("title-storage: %v", err)
	return status.Error(codes.Internal, "title storage error")
}
