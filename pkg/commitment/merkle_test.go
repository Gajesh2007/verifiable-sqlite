package commitment

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHashWithDomain(t *testing.T) {
	// Test that domain separation works as expected
	data := []byte("test data")

	hash1 := hashWithDomain("domain1:", data)
	hash2 := hashWithDomain("domain2:", data)

	// Different domains should produce different hashes
	assert.NotEqual(t, hash1, hash2)

	// Same domain and data should produce same hash
	hash1Repeat := hashWithDomain("domain1:", data)
	assert.Equal(t, hash1, hash1Repeat)
}

func TestBuildMerkleTree(t *testing.T) {
	tests := []struct {
		name          string
		leaves        [][]byte
		expectNilRoot bool
	}{
		{
			name:          "empty leaves",
			leaves:        [][]byte{},
			expectNilRoot: false, // Should return an empty tree hash, not nil
		},
		{
			name: "single leaf",
			leaves: [][]byte{
				[]byte("leaf1"),
			},
			expectNilRoot: false,
		},
		{
			name: "two leaves",
			leaves: [][]byte{
				[]byte("leaf1"),
				[]byte("leaf2"),
			},
			expectNilRoot: false,
		},
		{
			name: "odd number of leaves",
			leaves: [][]byte{
				[]byte("leaf1"),
				[]byte("leaf2"),
				[]byte("leaf3"),
			},
			expectNilRoot: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := BuildMerkleTree(tc.leaves)
			if tc.expectNilRoot {
				assert.Nil(t, root)
			} else {
				assert.NotNil(t, root)
				assert.NotEmpty(t, root.Hash)
			}
		})
	}
}

func TestMerkleRootHex(t *testing.T) {
	// Test empty/nil case
	assert.Empty(t, MerkleRootHex(nil))

	// Test normal case
	leaves := [][]byte{[]byte("data1"), []byte("data2")}
	root := BuildMerkleTree(leaves)
	rootHex := MerkleRootHex(root)

	// Verify it's a valid hex string
	_, err := hex.DecodeString(rootHex)
	assert.NoError(t, err)
	assert.NotEmpty(t, rootHex)
}

func TestDeterminism(t *testing.T) {
	// Verify that the same inputs always produce the same Merkle root
	leaves1 := [][]byte{[]byte("data1"), []byte("data2"), []byte("data3")}
	leaves2 := [][]byte{[]byte("data1"), []byte("data2"), []byte("data3")}

	root1 := BuildMerkleTree(leaves1)
	root2 := BuildMerkleTree(leaves2)

	assert.Equal(t, MerkleRootHex(root1), MerkleRootHex(root2))

	// Verify that different inputs produce different Merkle roots
	leaves3 := [][]byte{[]byte("data1"), []byte("data2"), []byte("data4")} // Changed one leaf

	root3 := BuildMerkleTree(leaves3)
	assert.NotEqual(t, MerkleRootHex(root1), MerkleRootHex(root3))
}