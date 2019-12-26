// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package encrypt

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"

	"github.com/pingcap/errors"
)

var (
	secretKey, _ = hex.DecodeString("a529b7665997f043a30ac8fadcb51d6aa032c226ab5b7750530b12b8c1a16a48")
	ivSep        = []byte("@") // ciphertext format: iv + ivSep + encrypted-plaintext
)

// SetSecretKey sets the secret key which used to encrypt
func SetSecretKey(key []byte) error {
	switch len(key) {
	case 16, 24, 32:
		break
	default:
		return errors.Errorf("secretKey not valid: %v", key)
	}
	secretKey = key
	return nil
}

// Encrypt tries to encrypt plaintext to base64 encoded ciphertext
func Encrypt(plaintext string) (string, error) {
	ciphertext, err := encrypt([]byte(plaintext))
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Decrypt tries to decrypt base64 encoded ciphertext to plaintext
func Decrypt(ciphertextB64 string) (string, error) {
	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextB64)
	if err != nil {
		return "", errors.Annotatef(err, "base 64 failed to decode: %s", ciphertext)
	}

	plaintext, err := decrypt(ciphertext)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(plaintext), nil
}

// encrypt encrypts plaintext to ciphertext
func encrypt(plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(secretKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	iv, err := genIV(block.BlockSize())
	if err != nil {
		return nil, err
	}

	ciphertext := make([]byte, 0, len(iv)+len(ivSep)+len(plaintext))
	ciphertext = append(ciphertext, iv...)
	ciphertext = append(ciphertext, ivSep...)
	ciphertext = append(ciphertext, plaintext...) // will be overwrite by XORKeyStream

	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(ciphertext[len(iv)+len(ivSep):], plaintext)

	return ciphertext, nil
}

// decrypt decrypts ciphertext to plaintext
func decrypt(ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(secretKey)
	if err != nil {
		return nil, err
	}

	if len(ciphertext) < block.BlockSize()+len(ivSep) {
		// return nil, terror.ErrCiphertextLenNotValid.Generate(block.BlockSize()+len(ivSep), len(ciphertext))
		return nil, errors.Errorf("ciphertext not valid")
	}

	if !bytes.Equal(ciphertext[block.BlockSize():block.BlockSize()+len(ivSep)], ivSep) {
		// return nil, terror.ErrCiphertextContextNotValid.Generate()
		return nil, errors.Errorf("ciphertext not valid")
	}

	iv := ciphertext[:block.BlockSize()]
	ciphertext = ciphertext[block.BlockSize()+len(ivSep):]
	plaintext := make([]byte, len(ciphertext))

	stream := cipher.NewCFBDecrypter(block, iv)
	stream.XORKeyStream(plaintext, ciphertext)

	return plaintext, nil
}

func genIV(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	return b, errors.Trace(err)
}
