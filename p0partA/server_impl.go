package p0partA

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"p0partA/kvstore"
	"strconv"
)

type keyValueServer struct {
	store          kvstore.KVStore
	clientChannels map[net.Conn]chan string
	listener       net.Listener
	activeCount    int
	droppedCount   int
	closed         bool
	commandChan    chan clientCommand
}

type clientCommand struct {
	conn    net.Conn
	action  string
	message string
}

func New(store kvstore.KVStore) KeyValueServer {
	kvs := &keyValueServer{
		store:          store,
		clientChannels: make(map[net.Conn]chan string),
		activeCount:    0,
		droppedCount:   0,
		closed:         false,
		commandChan:    make(chan clientCommand, 100), 
	}
	return kvs
}

func (kvs *keyValueServer) Start(port int) error {
	if kvs.closed {
		return fmt.Errorf("server has already been closed")
	}

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return fmt.Errorf("unable to start server: %v", err)
	}

	kvs.listener = listener
	fmt.Printf("Server started on port %d\n", port)

	go kvs.runServer()
	return nil
}

func (kvs *keyValueServer) Close() {
	if kvs.closed {
		return
	}
	kvs.closed = true
	if kvs.listener != nil {
		kvs.listener.Close()
	}
	kvs.commandChan <- clientCommand{action: "close"}
}

func (kvs *keyValueServer) CountActive() int {
	return kvs.activeCount
}

func (kvs *keyValueServer) CountDropped() int {
	return kvs.droppedCount
}


func (kvs *keyValueServer) runServer() {
	go func() {
		for {
			conn, err := kvs.listener.Accept()
			if err != nil {
				if kvs.closed {
					return
				}
				fmt.Println("Error accepting connection:", err)
				continue
			}
			kvs.commandChan <- clientCommand{conn: conn, action: "accept"}
		}
	}()

	for {
		cmd := <-kvs.commandChan
		switch cmd.action {
		case "accept":
			kvs.handleNewClient(cmd.conn)
		case "read":
			kvs.handleRead(cmd.conn, cmd.message)
		case "close":
			if cmd.conn != nil {
				kvs.handleClientClose(cmd.conn)
			} else {
				for conn := range kvs.clientChannels {
					kvs.handleClientClose(conn)
				}
				fmt.Println("Server shut down.")
				return
			}
		}
	}
}

func (kvs *keyValueServer) handleNewClient(conn net.Conn) {
	clientChan := make(chan string, 500)
	kvs.clientChannels[conn] = clientChan
	kvs.activeCount++

	go func() {
		reader := bufio.NewReader(conn)
		for {
			message, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					kvs.commandChan <- clientCommand{conn: conn, action: "close"}
					return
				}
				errBytes := []byte(err.Error())
				closedBytes := []byte("closed")
				for i := 0; i < len(errBytes)-len(closedBytes)+1; i++ {
					match := true
					for j := 0; j < len(closedBytes); j++ {
						if errBytes[i+j] != closedBytes[j] {
							match = false
							break
						}
					}
					if match {
						kvs.commandChan <- clientCommand{conn: conn, action: "close"}
						return
					}
				}
				fmt.Println("Error reading message:", err)
				kvs.commandChan <- clientCommand{conn: conn, action: "close"}
				return
			}

			msgBytes := []byte(message)
			start, end := 0, len(msgBytes)
			for start < end && (msgBytes[start] == ' ' || msgBytes[start] == '\t' || msgBytes[start] == '\n' || msgBytes[start] == '\r') {
				start++
			}
			for end > start && (msgBytes[end-1] == ' ' || msgBytes[end-1] == '\t' || msgBytes[end-1] == '\n' || msgBytes[end-1] == '\r') {
				end--
			}
			if start >= end {
				continue
			}
			trimmedMsg := string(msgBytes[start:end])

			kvs.commandChan <- clientCommand{conn: conn, action: "read", message: trimmedMsg}
		}
	}()

	go func() {
		for {
			message, ok := <-clientChan
			if !ok {
				return
			}
			if ch, exists := kvs.clientChannels[conn]; exists {
				msgBytes := []byte(message)
				if len(msgBytes) == 0 || msgBytes[len(msgBytes)-1] != '\n' {
					msgBytes = append(msgBytes, '\n')
				}
				// Only write if channel isn’t full, simulating slow client drop
				if len(ch) < cap(ch) {
					_, err := conn.Write(msgBytes)
					if err != nil {
						if !kvs.closed {
							fmt.Println("Error writing to client:", err)
						}
						kvs.commandChan <- clientCommand{conn: conn, action: "close"}
						return
					}
				}
			}
		}
	}()
}

func (kvs *keyValueServer) handleClientClose(conn net.Conn) {
	if ch, exists := kvs.clientChannels[conn]; exists {
		conn.Close()
		close(ch)
		delete(kvs.clientChannels, conn)
		kvs.activeCount--
		kvs.droppedCount++
	}
}

func (kvs *keyValueServer) handleRead(conn net.Conn, message string) {
	parts := bytes.Split([]byte(message), []byte(":"))
	if len(parts) < 2 {
		fmt.Println("Error processing message: invalid command format:", message)
		return
	}
	command := string(parts[0])

	switch command {
	case "Put":
		if len(parts) != 3 {
			fmt.Println("Error processing message: invalid Put command:", message)
			return
		}
		key := string(parts[1])
		value := parts[2]
		kvs.store.Put(key, value)

	case "Get":
		if len(parts) != 2 {
			fmt.Println("Error processing message: invalid Get command:", message)
			return
		}
		key := string(parts[1])
		values := kvs.store.Get(key)
		if ch, ok := kvs.clientChannels[conn]; ok {
			for _, v := range values {
				vBytes := bytes.TrimRight(v, " \t\n\r")
				response := fmt.Sprintf("%s:%s", key, string(vBytes))
				if len(ch) < cap(ch) {
					ch <- response
				}
			}
		}

	case "Delete":
		if len(parts) != 2 {
			fmt.Println("Error processing message: invalid Delete command:", message)
			return
		}
		key := string(parts[1])
		kvs.store.Delete(key)

	case "Update":
		if len(parts) != 4 {
			fmt.Println("Error processing message: invalid Update command:", message)
			return
		}
		key := string(parts[1])
		oldValue := parts[2]
		newValue := parts[3]
		kvs.store.Update(key, oldValue, newValue)

	default:
		fmt.Println("Error processing message: unknown command:", command)
	}
}
