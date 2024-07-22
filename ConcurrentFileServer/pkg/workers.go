package pkg

type WorkerPool struct {
	channel      chan func()
	workersCount int
}

func NewWorkerPool(workersCount int) *WorkerPool {
	channel := make(chan func(), 10000)
	result := &WorkerPool{channel: channel, workersCount: workersCount}

	for i := 0; i < workersCount; i++ {
		go result.process(i)
	}

	return result
}

func (p *WorkerPool) SubmitJob(job func()) {
	p.channel <- job
}

func (p *WorkerPool) process(id int) {
	for job := range p.channel {
		job()
	}
}
