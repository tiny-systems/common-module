package split

import (
	"context"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/utils"
	"reflect"
	"testing"
)

type h func(ctx context.Context, handler module.Handler, port string, msg interface{}) any

func TestSplit_Handle(t1 *testing.T) {
	type args struct {
		ctx     context.Context
		handler module.Handler
		port    string
		msg     interface{}
	}

	tests := []struct {
		name       string
		handleFunc func(t1 *testing.T, handle h) any
		wantErr    bool
	}{
		{
			name:    "test invalid message",
			wantErr: true,
			handleFunc: func(t *testing.T, handle h) any {
				return handle(context.Background(), func(ctx context.Context, port string, data interface{}) any {
					// should not be triggered cause message is invalid
					t.Error("output handler should not be triggered if income message is invalid")
					return nil
				}, "", 1)

			},
		},
		{
			name:    "OK empty message",
			wantErr: false,
			handleFunc: func(t *testing.T, handle h) any {
				return handle(context.Background(), func(ctx context.Context, port string, data interface{}) any {
					// should not be triggered cause message is invalid
					t.Error("output handler should not be triggered if income message is empty")
					return nil
				}, "", InMessage{})
			},
		},
		{
			name: "OK message",
			handleFunc: func(t *testing.T, handle h) any {
				var counter int

				var msg = InMessage{
					Array: []ItemContext{
						1, 2, 5,
					},
					Context: 42,
				}
				var length = len(msg.Array)

				defer func() {
					if counter != length {
						t.Errorf("handler should be trigged exactly %d times, but insted called %d", length, counter)
					}
				}()

				var resp = func(ctx context.Context, port string, data interface{}) any {
					counter++
					if port != OutPort {
						t1.Fatalf("invalid output port: %v", port)
					}
					resp, ok := data.(OutMessage)
					if !ok {
						t1.Fatalf("invalid type of response: %v", resp)
					}
					if !reflect.DeepEqual(resp.Context, msg.Context) {
						t1.Errorf("input and output context are not equal")
					}
					if resp.Item == 1 || resp.Item == 2 || resp.Item == 5 {
						//all good
						return nil
					}
					t1.Errorf("unexpected split output: %v", data)
					return nil
				}
				handle(context.Background(), resp, "", msg)
				return nil
			},
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &Component{}
			if resp := tt.handleFunc(t1, t.Handle); (utils.CheckForError(resp) != nil) != tt.wantErr {
				t1.Errorf("Handle() error = %v, wantErr %v", resp, tt.wantErr)
			}
		})
	}
}
