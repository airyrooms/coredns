package file

import (
	"bufio"
	"encoding/csv"
	"io"
	"log"
	"os"
	"path"
	"strconv"

	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/pkg/dnsutil"
	"github.com/coredns/coredns/plugin/pkg/parse"
	"github.com/coredns/coredns/plugin/proxy"

	"github.com/mholt/caddy"
)

func init() {
	caddy.RegisterPlugin("file", caddy.Plugin{
		ServerType: "dns",
		Action:     setup,
	})
}

func setup(c *caddy.Controller) error {
	zones, weights, err := fileWeightParse(c)
	if err != nil {
		return plugin.Error("file", err)
	}

	// Add startup functions to notify the master(s).
	for _, n := range zones.Names {
		z := zones.Z[n]
		c.OnStartup(func() error {
			z.StartupOnce.Do(func() {
				if len(z.TransferTo) > 0 {
					z.Notify()
				}
				z.Reload()
			})
			return nil
		})
	}

	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		return File{Next: next, Zones: zones, Weights: weights}
	})

	return nil
}

func fileWeightParse(c *caddy.Controller) (Zones, Weights, error) {
	z := make(map[string]*Zone)
	w := make(map[string]*Weight)
	names := []string{}
	servers := map[string]*WeightedServer{}
	origins := []string{}

	config := dnsserver.GetConfig(c)

	for c.Next() {
		// file db.file [zones...]
		if !c.NextArg() {
			return Zones{}, Weights{}, c.ArgErr()
		}
		fileName := c.Val()

		origins = make([]string, len(c.ServerBlockKeys))
		copy(origins, c.ServerBlockKeys)
		args := c.RemainingArgs()
		if len(args) > 0 {
			origins = args
		}

		if !path.IsAbs(fileName) && config.Root != "" {
			fileName = path.Join(config.Root, fileName)
		}

		reader, err := os.Open(fileName)
		if err != nil {
			// bail out
			return Zones{}, Weights{}, err
		}

		for i := range origins {
			origins[i] = plugin.Host(origins[i]).Normalize()
			zone, err := Parse(reader, origins[i], fileName, 0)
			//log.Println("Zone is: ", zone)
			if err == nil {
				z[origins[i]] = zone
			} else {
				return Zones{}, Weights{}, err
			}
			names = append(names, origins[i])
		}

		noReload := false
		prxy := proxy.Proxy{}
		t := []string{}
		var e error

		for c.NextBlock() {
			switch c.Val() {
			case "transfer":
				t, _, e = parse.Transfer(c, false)
				if e != nil {
					return Zones{}, Weights{}, e
				}

			case "no_reload":
				noReload = true

			case "weight":
				if !c.NextArg() {
					return Zones{}, Weights{}, c.ArgErr()
				}
				weightFile := c.Val()

				//log.Println("Weight File is: ", weightFile)

				if !path.IsAbs(weightFile) && config.Root != "" {
					weightFile = path.Join(config.Root, weightFile)
				}

				csvFile, err := os.Open(weightFile)
				defer csvFile.Close()
				if err != nil {
					// bail out
					//log.Println("Weightfile not accesible!")
					return Zones{}, Weights{}, err
				}

				reader := csv.NewReader(bufio.NewReader(csvFile))
				for {
					line, error := reader.Read()
					if error == io.EOF {
						break
					} else if error != nil {
						log.Fatal(error)
					}
					orig := line[0]
					w1, _ := strconv.Atoi(line[1])
					w2, _ := strconv.Atoi(line[2])
					weight := Weight{
						origin:     orig,
						weightEC2:  w1,
						weightKube: w2,
					}
					w[orig] = &weight
					server := &WeightedServer{Name: orig}
					servers[orig] = server
				}

				//for k, v := range w {
				//	log.Printf("key [%s] value [%s]\n", k, v.printWeight())
				//}

				//log.Println("Conjuring up weighted servers. . . ")

				//log.Println("Our conjured weighted servers are: ")

				//for _, v := range servers {
				//	log.Println(v.Name)
				//}

				//TODO setup weighted servers names and corresponding weights here, instead of in cnamelb.go
				//log.Println("Setting up weighted servers. . . ")
				//for _, srv := range servers {
				//	for k, v := range w {
				//		log.Printf("key [%s] value [%s]\n", k, v.printWeight())
				//	}
				//}

			case "upstream":
				args := c.RemainingArgs()
				if len(args) == 0 {
					return Zones{}, Weights{}, c.ArgErr()
				}
				ups, err := dnsutil.ParseHostPortOrFile(args...)
				if err != nil {
					return Zones{}, Weights{}, err
				}
				prxy = proxy.NewLookup(ups)
			default:
				return Zones{}, Weights{}, c.Errf("unknown property '%s'", c.Val())
			}

			for _, origin := range origins {
				if t != nil {
					z[origin].TransferTo = append(z[origin].TransferTo, t...)
				}
				z[origin].NoReload = noReload
				z[origin].Proxy = prxy
			}
		}
	}

	return Zones{Z: z, Names: names}, Weights{W: w, WeightedServer: servers}, nil
}
