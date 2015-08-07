package tunnel

import (
    "io"
    "os"
    "fmt"
    "net"
    "log"
    "time"
    "bytes"
    "net/url"
    "bufio"
    "strings"
    "strconv"
    "math/rand"
    "sync/atomic"
)

var gfwList []string

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
    daddr *net.TCPAddr
    clientMode bool
    cryptoMethod string
    gfwfile string
    secret []byte
    sessionsCount int32
    pool *recycler
}

func NewTunnel(faddr, daddr, baddrs string, clientMode bool, cryptoMethod, secret, gfwfile string, size uint32) *Tunnel {
    a1, err := net.ResolveTCPAddr("tcp", faddr)
    if err != nil {
        log.Fatalln("resolve frontend error:", err)
    }
    a3, err := net.ResolveTCPAddr("tcp", daddr)
    if err != nil {
        log.Fatalln("resolve direct error:", err)
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
        daddr: a3,
        clientMode: clientMode,
        cryptoMethod: cryptoMethod,
        gfwfile: gfwfile,
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
    var conn2 *net.TCPConn
    var err error
    var direct bool
    in, dhost, data := t.inGFW(conn)
    if !in {
        log.Println(dhost, "not in GFW, front:", conn.RemoteAddr().String(), "backend:", t.daddr.String())
        conn2, err = net.DialTCP("tcp", nil, t.daddr)
        if err != nil {
            log.Print(err)
            return
        }
        direct = true
    } else {
        baddr, err0 := t.baddrs.chooseBackend(t.baddrs)
        if err0 != nil {
            log.Print(err0)
            return
        }
        log.Println(dhost, "in GFW, front:", conn.RemoteAddr().String(), "backend:", baddr.String())
        conn2, err = net.DialTCP("tcp", nil, baddr)
        if err != nil {
            log.Print(err)
            return
        }
    }

    start := time.Now()
    connectTime := time.Now().Sub(start)
    start = time.Now()
    cipher := NewCipher(t.cryptoMethod, t.secret)
    readChan := make(chan int64)
    writeChan := make(chan int64)
    var readBytes, writeBytes int64
    writeBytes = int64(len(data))
    atomic.AddInt32(&t.sessionsCount, 1)

    var bconn, fconn *Conn
    if t.clientMode {
        fconn = NewConn(conn, nil, t.pool)
        if direct {
            bconn = NewConn(conn2, nil, t.pool)
        } else {
            bconn = NewConn(conn2, cipher, t.pool)
        }
        bconn.Write(data)
    } else {
        fconn = NewConn(conn, cipher, t.pool)
        bconn = NewConn(conn2, nil, t.pool)
    }
    go t.pipe(bconn, fconn, writeChan)
    go t.pipe(fconn, bconn, readChan)
    readBytes = <-readChan
    writeBytes += <-writeChan
    transferTime := time.Now().Sub(start)
    log.Printf("r:%d w:%d ct:%.3f t:%.3f [#%d]", readBytes, writeBytes,
        connectTime.Seconds(), transferTime.Seconds(), t.sessionsCount)
    atomic.AddInt32(&t.sessionsCount, -1)
}

func (t *Tunnel) Start() {
    t.loadGFWFileList()
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

func (t *Tunnel) inGFW(conn net.Conn) (in bool, dhost string, d []byte) {
	var i int
	reader := bufio.NewReader(conn)
	for {
		line, isMore, err := reader.ReadLine()
		d = append(d, line...)
        d = append(d, []byte("\r\n")...)

		if err != nil {
			break
		}

		if i <= 0 {
			if barr := bytes.Split(line, []byte(" ")); len(barr) >= 2 {
				hurl := barr[1]
				if 0 == strings.Index(string(hurl), "http") {
					u, _ := url.Parse(string(hurl))
					dhost = u.Host
					break
				}
			}
		}

		i++

		if isMore {
			break
		}

		if barr := bytes.Split(line, []byte(":")); i > 0 && len(barr) >= 2 {
			if strings.ToLower(string(barr[0])) == "host" {
				dhost = strings.TrimSpace(string(barr[1]))
				break
			}
		}
	}

	lastBuf := make([]byte, 1024*1024*2)
	if nr, err := reader.Read(lastBuf); err == nil {
		lastBuf = lastBuf[0:nr]
	}

    if !isIpAddress(dhost) {
        if arr := strings.Split(dhost, "."); len(arr) >= 2 {
            dhost = fmt.Sprintf("%s.%s", arr[len(arr)-2], arr[len(arr)-1])
        }
    }

	d = append(d, lastBuf...)

    if len(dhost) < 1 {
        log.Println("host not found", string(d[0:200]))
    }

    if len(dhost) < 1 || inStringArray(dhost, gfwList) {
        in = true
    }
	return
}

func (t *Tunnel) loadGFWFileList() {
    file, err := os.Open(t.gfwfile)
    if err != nil {
        log.Fatal(err.Error())
        return
    }
    defer file.Close()
    reader := bufio.NewReader(file)

    for {
        line, prefix, err := reader.ReadLine()
        if err != nil {
            break
        }
        if prefix {
            break
        }
        gfwList = append(gfwList, string(line))
    }
    log.Println("load gfw list length:", len(gfwList))
}

// range [min, max]
func random(min, max int) int {
    max = max + 1
    rand.Seed(time.Now().UnixNano())
    return rand.Intn(max - min) + min
}

func isIpAddress(host string) bool {
    h := strings.Replace(host, ".", "", -1)
    if _, err := strconv.Atoi(h); err == nil {
        return true
    }
    return false
}

func inStringArray(find string, arr []string) bool {
    for _, ele := range arr {
        if find == ele {
            return true
        }
    }
    return false
}
