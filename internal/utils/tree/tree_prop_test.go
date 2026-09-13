package tree

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/olicesx/quic-go/internal/protocol"
	"github.com/olicesx/quic-go/internal/utils"
)

// Differential property test of the fork's AVL tree against a brute-force
// reference, using the same value type the QUIC receive path uses
// (utils.ByteInterval, ordered by Start, matched by overlap). The tree is new
// fork code behind frame_sorter.go, so Len/Values/Head/Tail/Match/Delete must
// agree with a linear scan after every mutation and for every query.
func TestTreeDifferentialAgainstReference(t *testing.T) {
	rng := rand.New(rand.NewSource(20260913))
	tr := New[utils.ByteInterval]()
	var ref []utils.ByteInterval

	sortRef := func() {
		sort.Slice(ref, func(i, j int) bool { return ref[i].Comp(ref[j]) < 0 })
	}
	// validate walks the tree and asserts the BST ordering, the stored heights
	// and the AVL balance condition. Len/Values can agree with the reference
	// while the shape is wrong, so this is what catches a wrong turn in insert
	// or a weakened rebalance.
	var validate func(n *Node[utils.ByteInterval], lo, hi *utils.ByteInterval) int
	validate = func(n *Node[utils.ByteInterval], lo, hi *utils.ByteInterval) int {
		if n == nil {
			return 0
		}
		if lo != nil && n.Value.Comp(*lo) <= 0 {
			t.Fatalf("BST violation: %v is not greater than its lower bound %v", n.Value, *lo)
		}
		if hi != nil && n.Value.Comp(*hi) >= 0 {
			t.Fatalf("BST violation: %v is not smaller than its upper bound %v", n.Value, *hi)
		}
		lh := validate(n.left, lo, &n.Value)
		rh := validate(n.right, &n.Value, hi)
		if want := max(lh, rh) + 1; int(n.height) != want {
			t.Fatalf("node %v height=%d want %d", n.Value, n.height, want)
		}
		if diff := lh - rh; diff > 1 || diff < -1 {
			t.Fatalf("unbalanced node %v: left height %d, right height %d", n.Value, lh, rh)
		}
		return int(n.height)
	}
	check := func(step int, why string) {
		t.Helper()
		sortRef()
		if got := tr.Len(); got != len(ref) {
			t.Fatalf("%s step %d: Len=%d want %d", why, step, got, len(ref))
		}
		vals := tr.Values()
		if len(vals) != len(ref) {
			t.Fatalf("%s step %d: Values len=%d want %d", why, step, len(vals), len(ref))
		}
		for i := range ref {
			if vals[i] != ref[i] {
				t.Fatalf("%s step %d: Values[%d]=%v want %v", why, step, i, vals[i], ref[i])
			}
			if !tr.Contains(ref[i]) {
				t.Fatalf("%s step %d: Contains(%v)=false", why, step, ref[i])
			}
		}
		if len(ref) == 0 {
			if !tr.Empty() || tr.Head() != nil || tr.Tail() != nil {
				t.Fatalf("%s step %d: empty tree reports Head/Tail/Empty wrongly", why, step)
			}
			return
		}
		if h := tr.Head(); h == nil || *h != ref[0] {
			t.Fatalf("%s step %d: Head=%v want %v", why, step, h, ref[0])
		}
		if tl := tr.Tail(); tl == nil || *tl != ref[len(ref)-1] {
			t.Fatalf("%s step %d: Tail=%v want %v", why, step, tl, ref[len(ref)-1])
		}
		// Match must return every overlapping interval, in ascending order.
		q := utils.ByteInterval{
			Start: protocol.ByteCount(rng.Intn(200)),
			End:   protocol.ByteCount(rng.Intn(200) + 200),
		}
		var want []utils.ByteInterval
		for _, x := range ref {
			if x.Match(q) == 0 {
				want = append(want, x)
			}
		}
		got := tr.Match(q)
		if len(got) != len(want) {
			t.Fatalf("%s step %d: Match(%v) len=%d want %d (tree=%v ref=%v)", why, step, q, len(got), len(want), got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%s step %d: Match(%v)[%d]=%v want %v", why, step, q, i, got[i], want[i])
			}
		}
		validate(tr.root, nil, nil)
	}

	// Insert in shuffled order from a pool that contains equal keys and exact
	// duplicates, so insert's left turn, its duplicate-replacement branch and
	// the rotations after an out-of-order insert are all exercised. The tree
	// keeps one value per Comp class (the last one inserted), which is what
	// replicate mirrors.
	replicate := func(iv utils.ByteInterval) {
		for j := range ref {
			if ref[j].Comp(iv) == 0 {
				ref[j] = iv
				return
			}
		}
		ref = append(ref, iv)
	}
	var pool []utils.ByteInterval
	next := protocol.ByteCount(0)
	for i := 0; i < 200; i++ {
		next += protocol.ByteCount(rng.Intn(20) + 1)
		iv := utils.ByteInterval{Start: next, End: next + protocol.ByteCount(rng.Intn(15))}
		pool = append(pool, iv)
		switch i % 9 {
		case 0:
			// Exact duplicate: the duplicate branch must replace the value.
			pool = append(pool, iv)
		case 1:
			// Same Start, different End: still one Comp class.
			pool = append(pool, utils.ByteInterval{Start: iv.Start, End: iv.End + 5})
		case 2:
			// Touching interval, the shape the frame sorter creates.
			pool = append(pool, utils.ByteInterval{Start: iv.End, End: iv.End + 3})
		}
	}
	rng.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })
	for i, iv := range pool {
		tr.Insert(iv)
		replicate(iv)
		if i%17 == 0 {
			check(i, "insert")
		}
	}
	check(len(pool), "after inserts")

	// Delete in random order; the tree must track the reference exactly.
	toDelete := append([]utils.ByteInterval(nil), ref...)
	rng.Shuffle(len(toDelete), func(i, j int) { toDelete[i], toDelete[j] = toDelete[j], toDelete[i] })
	for i, iv := range toDelete {
		tr.Delete(iv)
		for j := range ref {
			if ref[j] == iv {
				ref = append(ref[:j], ref[j+1:]...)
				break
			}
		}
		if i%13 == 0 || i == len(toDelete)-1 {
			check(i, "delete")
		}
	}
	if !tr.Empty() {
		t.Fatal("tree not empty after deleting every interval")
	}

	// Reinsert after the drain: pooled nodes must not resurrect stale values.
	for i := 0; i < 50; i++ {
		iv := utils.ByteInterval{Start: protocol.ByteCount(i * 10), End: protocol.ByteCount(i*10 + 5)}
		tr.Insert(iv)
		ref = append(ref, iv)
	}
	check(0, "reinsert")
	if got := tr.Match(utils.ByteInterval{Start: 0, End: 1000}); len(got) != 50 {
		t.Fatalf("Match over the whole range returned %d intervals, want 50", len(got))
	}
}
