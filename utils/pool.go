package utils

type Reusable interface {
	ReleaseToPool()
}

func ReleaseToPool(pools ...Reusable) {
	for _, p := range pools {
		p.ReleaseToPool()
	}
}
