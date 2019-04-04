package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/nats-io/go-nats"
	"log"
	"os"
	"sync"
	"time"
)

const subj = "foo"

func sending(input <-chan struct{}, cancel <-chan struct{}) {
	for {
		select {
		case <-cancel:
			return
		case <-input:
			go func() {
				nc, err := nats.Connect(os.Getenv("NATS_URL"))
				defer nc.Close()
				if err != nil {
					log.Println(err)
					return
				}
				var wg sync.WaitGroup
				wg.Add(1)
				for i := 0; i < 10; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						bytes := make([]byte, 8)
						binary.LittleEndian.PutUint64(bytes, uint64(time.Now().UnixNano()))

						if err := nc.Publish(subj, bytes); err != nil {
							log.Println(err)
							return
						}
					}()
				}
				wg.Done()
				wg.Wait()
				err = nc.Drain()
				if err != nil {
					log.Println(err)
					return
				}
			}()
		}
	}
}

func inputChan(cancel <-chan struct{}) <-chan struct{} {
	input := make(chan struct{})
	reader := bufio.NewReader(os.Stdin)
	go func() {
		for {
			select {
			case <-cancel:
				return
			default:
				_, _, _ = reader.ReadRune()
				input <- struct{}{}
			}
		}
	}()
	return input
}

func main() {
	cancel := make(chan struct{})
	go sending(inputChan(cancel), cancel)

	nc, _ := nats.Connect(os.Getenv("NATS_URL"))

	sub, _ := nc.Subscribe(subj, func(msg *nats.Msg) {
		fmt.Printf("unix %d", binary.LittleEndian.Uint64(msg.Data))
	})

	<-time.After(10 * time.Second)
	close(cancel)

	// Unsubscribe
	sub.Unsubscribe()

	// Drain
	sub.Drain()

	// Drain connection (Preferred for responders)
	// Close() not needed if this is called.
	nc.Drain()

	// Close connection
	nc.Close()
	return
}
