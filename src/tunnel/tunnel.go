package tunnel

import (
    "io"
    "fmt"
    "net"
    "log"
    "time"
    "strings"
    "math/rand"
    "sync/atomic"
)

type backendAddrs []*net.TCPAddr

func (self backendAddrs) chooseBackend(bs []*net.TCPAddr) (b *net.TCPAddr, err error) {
    idx := random(0, len(bs) - 1)
    if idx < 0 {
        err = fmt.Errorf("backend hosts empty")
        return
    }
    return bs[idx], nil
}

type Tunnel struct {
    faddr *net.TCPAddr
    baddrs backendAddrs
    clientMode bool
    cryptoMethod string
    secret []byte
    sessionsCount int32
    pool *recycler
}

func NewTunnel(faddr, baddrs string, clientMode bool, cryptoMethod, secret string, size uint32) *Tunnel {
    a1, err := net.ResolveTCPAddr("tcp", faddr)
    if err != nil {
        log.Fatalln("resolve frontend error:", err)
    }

    baddrsArr := strings.Split(baddrs, ",")
    if len(baddrsArr) < 1 {
        log.Println("backend host is empty")
    }
    var a2s backendAddrs
    for _, baddr := range baddrsArr {
        a2, err := net.ResolveTCPAddr("tcp", baddr)
        if err != nil {
            log.Fatalln("resolve backend error:", err)
        }
        a2s = append(a2s, a2)
    }
    return &Tunnel{
        faddr: a1,
        baddrs: a2s,
        clientMode: clientMode,
        cryptoMethod: cryptoMethod,
        secret: []byte(secret),
        sessionsCount: 0,
        pool: NewRecycler(size),
    }
}

func (t *Tunnel) pipe(dst, src *Conn, c chan int64) {
    defer func() {
        dst.CloseWrite()
        src.CloseRead()
    }()
    n, err := io.Copy(dst, src)
    if err != nil {
        log.Print(err)
    }
    c <- n
}

func (t *Tunnel) transport(conn net.Conn) {
    start := time.Now()
    baddr, err0 := t.baddrs.chooseBackend(t.baddrs)
    if err0 != nil {
        log.Print(err0)
        return
    }
    log.Println("choose backend:", baddr.String())
    conn2, err := net.DialTCP("tcp", nil, baddr)
    if err != nil {
        log.Print(err)
        return
    }
    connectTime := time.Now().Sub(start)
    start = time.Now()
    cipher := NewCipher(t.cryptoMethod, t.secret)
    readChan := make(chan int64)
    writeChan := make(chan int64)
    var readBytes, writeBytes int64
    atomic.AddInt32(&t.sessionsCount, 1)
    var bconn, fconn *Conn
    if t.clientMode {
        fconn = NewConn(conn, nil, t.pool)
        bconn = NewConn(conn2, cipher, t.pool)
    } else {
        fconn = NewConn(conn, cipher, t.pool)
        bconn = NewConn(conn2, nil, t.pool)
    }
    go t.pipe(bconn, fconn, writeChan)
    go t.pipe(fconn, bconn, readChan)
    readBytes = <-readChan
    writeBytes = <-writeChan
    transferTime := time.Now().Sub(start)
    log.Printf("r:%d w:%d ct:%.3f t:%.3f [#%d]", readBytes, writeBytes,
        connectTime.Seconds(), transferTime.Seconds(), t.sessionsCount)
    atomic.AddInt32(&t.sessionsCount, -1)
}

func (t *Tunnel) Start() {
    ln, err := net.ListenTCP("tcp", t.faddr)
    if err != nil {
        log.Fatal(err)
    }
    defer ln.Close()

    for {
        conn, err := ln.AcceptTCP()
        if err != nil {
            log.Println("accept:", err)
            continue
        }
        go t.transport(conn)
    }
}

// range [min, max]
func random(min, max int) int {
    max = max + 1
    rand.Seed(time.Now().UnixNano())
    return rand.Intn(max - min) + min
}
