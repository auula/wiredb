// Copyright 2022 Leon Ding <ding_ms@outlook.com> https://wiredb.github.io

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"encoding/json"
	"errors"

	"github.com/vmihailenco/msgpack/v5"
)

type Collection struct {
	Collection []any  `json:"collection" bson:"collection" binding:"required"`
	TTL        uint64 `json:"ttl,omitempty"`
}

func NewCollection() *Collection {
	return new(Collection)
}

// AddItem 向 List 中添加新项目
func (cle *Collection) AddItem(item any) {
	cle.Collection = append(cle.Collection, item)
}

// Remove 从 List 中删除指定的项目
func (cle *Collection) Remove(item any) error {
	for i, v := range cle.Collection {
		if v == item {
			cle.Collection = append(cle.Collection[:i], cle.Collection[i+1:]...)
			return nil
		}
	}
	return errors.New("collection item not found")
}

// GetItem 获取 List 中指定索引的项目
func (cle *Collection) GetItem(index int) (any, error) {
	if index < 0 || index >= len(cle.Collection) {
		return nil, errors.New("collection index out of bounds")
	}
	return cle.Collection[index], nil
}

func (cle *Collection) Rnage(statIndex, endIndex int) ([]any, error) {
	var result []any
	for i, v := range cle.Collection {
		if i >= statIndex && i <= endIndex {
			result = append(result, v)
		}
	}
	return result, nil
}

func (cle *Collection) LPush(item any) {
	cle.Collection = append([]any{item}, cle.Collection...)
}

func (cle *Collection) RPush(item any) {
	cle.Collection = append(cle.Collection, item)
}

func (cle *Collection) Size() int {
	return len(cle.Collection)
}

func (cle *Collection) Clear() {
	cle.TTL = 0
	cle.Collection = make([]any, 0)
}

func (cle Collection) ToBytes() ([]byte, error) {
	return msgpack.Marshal(cle.Collection)
}

func (cle *Collection) ToJSON() ([]byte, error) {
	return json.Marshal(cle.Collection)
}
