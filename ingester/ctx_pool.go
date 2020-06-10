/* This file contains some modifications to the source code of VictoriaMetrics.
https://github.com/VictoriaMetrics/VictoriaMetrics/blob/9e8733ff65ac9a540b0d8367d97f501a965498f0/app/vminsert/common/insert_ctx_pool.go
*/
package ingester

import (
	"runtime"
	"sync"
)

// GetInsertCtx returns InsertCtx from the pool.
//
// Call PutInsertCtx for returning it to the pool.
func GetInsertCtx() *InsertCtx {
	select {
	case ctx := <-insertCtxPoolCh:
		return ctx
	default:
		if v := insertCtxPool.Get(); v != nil {
			return v.(*InsertCtx)
		}
		return &InsertCtx{}
	}
}

// PutInsertCtx returns ctx to the pool.
//
// ctx cannot be used after the call.
func PutInsertCtx(ctx *InsertCtx) {
	ctx.Reset(0)
	select {
	case insertCtxPoolCh <- ctx:
	default:
		insertCtxPool.Put(ctx)
	}
}

var insertCtxPool sync.Pool
var insertCtxPoolCh = make(chan *InsertCtx, runtime.GOMAXPROCS(-1))
