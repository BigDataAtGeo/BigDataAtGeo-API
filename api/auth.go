package api

import (
	"crypto/hmac"
	"fmt"
	"hash"
	"net/http"
)

type HmacSigner struct {
	publicKey    []byte
	privateKey   []byte
	hashFunc     func() hash.Hash
	headerPrefix string
}

func NewHmacSigner(publicKey, privateKey string, hashFunc func() hash.Hash) HmacSigner {
	return HmacSigner{
		publicKey:    []byte(publicKey),
		privateKey:   []byte(privateKey),
		hashFunc:     hashFunc,
		headerPrefix: "hmac " + publicKey + ":",
	}
}

func (signer *HmacSigner) Sign(header *http.Header, method []byte, endpoint []byte) {
	// little bit faster to copy than append
	message := make([]byte, len(method)+len(endpoint)+len(signer.publicKey))
	copy(message, method)
	copy(message[len(method):], endpoint)
	copy(message[len(method)+len(endpoint):], signer.publicKey)

	mac := hmac.New(signer.hashFunc, signer.privateKey)
	mac.Write(message)
	hexHash := fmt.Sprintf("%x", mac.Sum(nil))

	header.Set("Accept", "application/json")
	header.Set("Authorization", signer.headerPrefix + hexHash)
}
