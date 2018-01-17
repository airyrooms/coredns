package file

import (
	"regexp"
	"strconv"

	"github.com/miekg/dns"
)

// Wg is a interface that implement a weighted round robin algorithm.
// https://github.com/smallnest/weighted
type Wg interface {
	Next() string
	Add(server interface{}, wgWeight int)
	RemoveAll()
	Reset()
}

//Weight defines a domain's weight on double-deployments with CNAME loadbalancing
//Heavily crafted for Airy Engineering's double-deployment use case
type Weight struct {
	origin     string
	weightEC2  int
	weightKube int
}

//func (w *Weight) printWeight() string {
//	weightString := w.origin + strconv.Itoa(w.weightEC2) + strconv.Itoa(w.weightKube)
//	return weightString
//}

func (w *Weight) getEC2Weight() string {
	return strconv.Itoa(w.weightEC2)
}

func (w *Weight) getKubeWeight() string {
	return strconv.Itoa(w.weightKube)
}

// weighted2 is a wrapped server with weight that is used to implement LVS weighted round robin algorithm.
type weighted2 struct {
	Server interface {
		String() string
	}
	weightSize int
}

// WeightedServer is struct that contains weighted servers implement LVS weighted round robin algorithm.
//
// http://kb.linuxvirtualserver.org/wiki/Weighted_Round-Robin_Scheduling
// http://zh.linuxvirtualserver.org/node/37
type WeightedServer struct {
	servers []*weighted2
	Name    string
	n       int
	gcd     int
	maxW    int
	i       int
	cw      int
}

func (w *WeightedServer) String() string {
	return w.Name
}

// AiryServer implements Server interface, represents airy's backend CNAME DNS name
type AiryServer struct {
	Name string
}

func (a AiryServer) String() string {
	return a.Name
}

// Add a weighted server.
func (w *WeightedServer) Add(server AiryServer, weight int) {
	weighted := &weighted2{Server: server, weightSize: weight}
	if weight > 0 {
		if w.gcd == 0 {
			w.gcd = weight
			w.maxW = weight
			w.i = -1
			w.cw = 0
		} else {
			w.gcd = gcd(w.gcd, weight)
			if w.maxW < weight {
				w.maxW = weight
			}
		}
	}
	w.servers = append(w.servers, weighted)
	w.n++
}

// RemoveAll removes all weighted servers.
func (w *WeightedServer) RemoveAll() {
	w.servers = w.servers[:0]
	w.n = 0
	w.gcd = 0
	w.maxW = 0
	w.i = -1
	w.cw = 0
}

//Reset resets all current weights.
func (w *WeightedServer) Reset() {
	w.i = -1
	w.cw = 0
}

// Next returns next selected server.
func (w *WeightedServer) Next() string {
	if w.n == 0 {
		return ""
	}

	if w.n == 1 {
		return w.servers[0].Server.String()
	}

	for {
		w.i = (w.i + 1) % w.n
		if w.i == 0 {
			w.cw = w.cw - w.gcd
			if w.cw <= 0 {
				w.cw = w.maxW
				if w.cw == 0 {
					return ""
				}
			}
		}

		if w.servers[w.i].weightSize >= w.cw {
			return w.servers[w.i].Server.String()
		}
	}
}

func gcd(x, y int) int {
	var t int
	for {
		t = (x % y)
		if t > 0 {
			x = y
			y = t
		} else {
			return y
		}
	}
}

//modified for Airyrooms' use -> returns the first shuffled record
func roundRobinShuffle(records []dns.RR) dns.RR {
	switch l := len(records); l {
	case 0:
		break
	case 2:
		if dns.Id()%2 == 0 {
			records[0], records[1] = records[1], records[0]
		}
	default:
		for j := 0; j < l*(int(dns.Id())%4+1); j++ {
			q := int(dns.Id()) % l
			p := int(dns.Id()) % l
			if q == p {
				p = (p + 1) % l
			}
			records[q], records[p] = records[p], records[q]
		}
	}
	return records[0]
}

//CnameWRRShuffle is modified for Airyrooms' use -> returns cname record according to weights
func (w *Weights) CnameWRRShuffle(records []dns.RR) string {
	//log.Println("Resolving ", records[0].String())
	rname := records[0].Header().Name
	weights := w.W[rname]
	if wserver, ok := w.WeightedServer[rname]; ok {
		if len(wserver.servers) < 1 {
			//log.Println("Setting up weighted servers. . . ")
			for _, v := range records {
				name := v.(*dns.CNAME).Target
				isKube, _ := regexp.MatchString(".*svc.cluster.*", name)

				if isKube {
					wserver.Add(AiryServer{Name: name}, weights.weightKube)
				} else {
					wserver.Add(AiryServer{Name: name}, weights.weightEC2)
				}
			}

			//for _, servs := range wserver.servers {
			//	log.Println("The server is:", servs.Server.String())
			//}
		}
		returnServer := wserver.Next()
		return returnServer
	}
	return records[0].(*dns.CNAME).Target
}
