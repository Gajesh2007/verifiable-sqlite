package commitment

import (
	"crypto/sha256"
	"encoding/hex"
)

// Domain prefixes for domain separation in hashing
const (
	DomainRowID        = "row_id:"
	DomainLeaf         = "leaf:"
	DomainNode         = "node:"
	DomainTable        = "table:"
	DomainDatabase     = "db:"
)

// MerkleNode represents a node in a Merkle tree
type MerkleNode struct {
	Hash  []byte
	Left  *MerkleNode
	Right *MerkleNode
}

// hashWithDomain prefixes data with a domain before hashing
func hashWithDomain(domain string, data []byte) []byte {
	h := sha256.New()
	h.Write([]byte(domain))
	h.Write(data)
	return h.Sum(nil)
}

// BuildMerkleTree constructs a Merkle tree from a list of leaf values
func BuildMerkleTree(leaves [][]byte) *MerkleNode {
	if len(leaves) == 0 {
		// Return a hash of an empty tree
		return &MerkleNode{
			Hash: hashWithDomain(DomainNode, []byte("empty")),
		}
	}

	// Convert raw leaves to leaf nodes with domain separation
	var nodes []*MerkleNode
	for _, leaf := range leaves {
		nodes = append(nodes, &MerkleNode{
			Hash: hashWithDomain(DomainLeaf, leaf),
		})
	}

	// If we have an odd number of leaves, duplicate the last one
	if len(nodes)%2 == 1 {
		nodes = append(nodes, nodes[len(nodes)-1])
	}

	// Build the tree bottom-up
	for len(nodes) > 1 {
		var nextLevel []*MerkleNode
		for i := 0; i < len(nodes); i += 2 {
			left := nodes[i]
			right := nodes[i+1]
			
			// Concatenate the child hashes and hash them with domain separation
			combined := append(left.Hash, right.Hash...)
			newNode := &MerkleNode{
				Hash:  hashWithDomain(DomainNode, combined),
				Left:  left,
				Right: right,
			}
			nextLevel = append(nextLevel, newNode)
		}
		nodes = nextLevel
	}

	// The root is the only node left
	return nodes[0]
}

// MerkleRootHex returns the hexadecimal string representation of a Merkle root
func MerkleRootHex(root *MerkleNode) string {
	if root == nil {
		return ""
	}
	return hex.EncodeToString(root.Hash)
}