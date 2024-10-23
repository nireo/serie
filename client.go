package serie

// Here the client can be used as an interface to interact with the distributed nodes.
type Client struct {
	Distributor *Distributor
}

func Write(points []Point) error {
	return nil
}
