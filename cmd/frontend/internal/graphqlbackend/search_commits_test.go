package graphqlbackend

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"sourcegraph.com/sourcegraph/sourcegraph/cmd/frontend/internal/backend"
	"sourcegraph.com/sourcegraph/sourcegraph/cmd/frontend/internal/pkg/types"
	"sourcegraph.com/sourcegraph/sourcegraph/pkg/gitserver"
	"sourcegraph.com/sourcegraph/sourcegraph/pkg/searchquery"
	"sourcegraph.com/sourcegraph/sourcegraph/pkg/vcs"
	vcstesting "sourcegraph.com/sourcegraph/sourcegraph/pkg/vcs/testing"
)

func TestSearchCommitsInRepo(t *testing.T) {
	ctx := context.Background()

	var calledVCSRawLogDiffSearch bool
	calledRepoVCSOpen := backend.Mocks.Repos.MockVCS(t, "repo", vcstesting.MockRepository{
		RawLogDiffSearch_: func(ctx context.Context, opt vcs.RawLogDiffSearchOptions) ([]*vcs.LogCommitSearchResult, bool, error) {
			calledVCSRawLogDiffSearch = true
			if want := "p"; opt.Query.Pattern != want {
				t.Errorf("got %q, want %q", opt.Query.Pattern, want)
			}
			if want := []string{
				"--max-count=" + strconv.Itoa(defaultMaxSearchResults+1),
				"--unified=0",
				"--no-prefix",
				"--regexp-ignore-case",
				"rev",
				"--since=1 month ago",
			}; !reflect.DeepEqual(opt.Args, want) {
				t.Errorf("got %v, want %v", opt.Args, want)
			}
			return []*vcs.LogCommitSearchResult{
				{
					Commit: vcs.Commit{ID: "c1"},
					Diff:   &vcs.Diff{Raw: "x"},
				},
			}, true, nil
		},
	})

	query, err := searchquery.ParseAndCheck("p")
	if err != nil {
		t.Fatal(err)
	}
	repoRevs := repositoryRevisions{
		repo:          &types.Repo{ID: 1, URI: "repo"},
		gitserverRepo: gitserver.Repo{Name: "repo", URL: "u"},
		revs:          []revspecOrRefGlob{{revspec: "rev"}},
	}
	results, limitHit, timedOut, err := searchCommitsInRepo(ctx, commitSearchOp{
		repoRevs:          repoRevs,
		info:              &patternInfo{Pattern: "p", FileMatchLimit: int32(defaultMaxSearchResults)},
		query:             *query,
		diff:              true,
		textSearchOptions: vcs.TextSearchOptions{Pattern: "p"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []*commitSearchResultResolver{
		{
			commit: &gitCommitResolver{
				repo:   &repositoryResolver{repo: &types.Repo{ID: 1, URI: "repo"}},
				oid:    "c1",
				author: *toSignatureResolver(&vcs.Signature{}),
			},
			diffPreview: &highlightedString{value: "x", highlights: []*highlightedRange{}},
		},
	}; !reflect.DeepEqual(results, want) {
		t.Errorf("results\ngot  %v\nwant %v", results, want)
	}
	if limitHit {
		t.Error("limitHit")
	}
	if timedOut {
		t.Error("timedOut")
	}
	if !*calledRepoVCSOpen {
		t.Error("!calledRepoVCSOpen")
	}
	if !calledVCSRawLogDiffSearch {
		t.Error("!calledVCSRawLogDiffSearch")
	}
}

func (c *commitSearchResultResolver) String() string {
	return fmt.Sprintf("{commit: %+v diffPreview: %+v messagePreview: %+v}", c.commit, c.diffPreview, c.messagePreview)
}
