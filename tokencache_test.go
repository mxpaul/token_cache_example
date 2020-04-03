package tokencache

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

type App string
type Token string

type TokenCache interface {
	LockAndRead(app App) Token
	SaveAndUnlock(app App, token Token)
	Get(app App) Token
	Put(app App, token Token)
}

// -----------------------------------------------------------
// Single Mutex for entire map implementation
// -----------------------------------------------------------
type TokenCacheSingleMutex struct {
	TokenCache
	mu    sync.Mutex
	cache map[App]Token
}

func (c TokenCacheSingleMutex) LockAndRead(app App) (t Token) {
	c.mu.Lock()
	t = c.cache[app]
	return
}

func (c TokenCacheSingleMutex) Get(app App) (t Token) {
	c.mu.Lock()
	t = c.cache[app]
	c.mu.Unlock()
	return
}

func (c TokenCacheSingleMutex) Put(app App, t Token) {
	c.mu.Lock()
	c.cache[app] = t
	c.mu.Unlock()
}

func (c TokenCacheSingleMutex) SaveAndUnlock(app App, t Token) {
	c.cache[app] = t
	c.mu.Unlock()
}

// -----------------------------------------------------------
// Mutex per each key implementation
// -----------------------------------------------------------
type LockableToken struct {
	token Token
	mu    sync.Mutex
}

// Very unsafe implementation, just for completness
type TokenCacheMutexPerKey struct {
	TokenCache
	// FIXME: we have to lock map before accessing it's keys!
	cache map[App]LockableToken
}

func (c TokenCacheMutexPerKey) LockAndRead(app App) (t Token) {
	if lockToken, ok := c.cache[app]; ok {
		lockToken.mu.Lock()
		t = lockToken.token
	} else {
		c.cache[app] = LockableToken{}
		return c.LockAndRead(app)
	}
	return
}

func (c TokenCacheMutexPerKey) Get(app App) (t Token) {
	if lockToken, ok := c.cache[app]; ok {
		lockToken.mu.Lock()
		t = lockToken.token
		lockToken.mu.Unlock()
	}
	return
}

func (c TokenCacheMutexPerKey) Put(app App, t Token) {
	if lockToken, ok := c.cache[app]; ok {
		lockToken.mu.Lock()
		lockToken.token = t
		lockToken.mu.Unlock()
	} else {
		c.cache[app] = LockableToken{}
		c.Put(app, t)
	}
}

func (c TokenCacheMutexPerKey) SaveAndUnlock(app App, t Token) {
	if lockToken, ok := c.cache[app]; ok {
		lockToken.token = t
		lockToken.mu.Unlock()
	} else {
		panic("can not unlock non-existing mutex")
	}
}

// -----------------------------------------------------------
// Channel implementation
// -----------------------------------------------------------
type TokenCacheChannel struct {
	TokenCache
	mu    sync.Mutex
	cache map[App]chan Token
}

func (c TokenCacheChannel) LockAndRead(app App) (t Token) {
	c.mu.Lock()
	if ch, ok := c.cache[app]; ok {
		c.mu.Unlock()
		t = <-ch
	} else {
		c.cache[app] = make(chan Token, 1)
		c.mu.Unlock()
	}
	return
}

func (c TokenCacheChannel) Get(app App) (t Token) {
	c.mu.Lock()
	if ch, ok := c.cache[app]; ok {
		c.mu.Unlock()
		t = <-ch
		ch <- t
	}
	return
}

func (c TokenCacheChannel) Put(app App, t Token) {
	c.mu.Lock()
	if ch, ok := c.cache[app]; ok {
		c.mu.Unlock()
		<-ch
		ch <- t
	} else {
		ch := make(chan Token, 1)
		c.cache[app] = ch
		c.mu.Unlock()
		ch <- t
	}
}

func (c TokenCacheChannel) SaveAndUnlock(app App, t Token) {
	c.mu.Lock()
	if ch, ok := c.cache[app]; ok {
		c.mu.Unlock()
		ch <- t
	} else {
		panic("WTF!")
	}
}

// -----------------------------------------------------------
// Helper to run benchmarks
// -----------------------------------------------------------
type Tester struct {
	numApps    int
	numWorkers int
	apps       []App
	workerChan []chan App
	wg         sync.WaitGroup
}
type TesterWorker func(cache TokenCache, app App)

func StartNewTester(numApps, numWorkers int, cache TokenCache, worker TesterWorker) *Tester {
	tester := Tester{
		numApps:    numApps,
		numWorkers: numWorkers,
	}

	tester.apps = make([]App, 0, tester.numApps)

	for i := 0; i < tester.numApps; i++ {
		app := App(fmt.Sprintf("app%04d", i))
		token := Token(strings.Repeat(fmt.Sprintf("%d", i%9), 100))
		tester.apps = append(tester.apps, app)
		cache.Put(app, token)
	}

	tester.wg.Add(numWorkers)
	tester.workerChan = make([]chan App, tester.numWorkers)
	for i := 0; i < tester.numWorkers; i++ {
		tester.workerChan[i] = make(chan App)
		go func(ch <-chan App, wg *sync.WaitGroup) {
			for app := range ch {
				worker(cache, app)
			}
			tester.wg.Done()
		}(tester.workerChan[i], &tester.wg)
	}
	return &tester
}

func (tester *Tester) PutJob(i int) {
	appIndex := i % tester.numApps
	workerIndex := i % tester.numWorkers
	app := tester.apps[appIndex]
	tester.workerChan[workerIndex] <- app
}

func (tester *Tester) Stop(b *testing.B) {
	b.StopTimer()
	for _, ch := range tester.workerChan {
		close(ch)
	}
	b.StartTimer()
	tester.wg.Wait()
}

func (tester *Tester) RunBenchmark(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tester.PutJob(i)
	}

	tester.Stop(b)
}

// -----------------------------------------------------------
// Workers to test different use cases or maybe load profiles
// -----------------------------------------------------------
func WorkerGet(cache TokenCache, app App) {
	cache.Get(app)
}
func WorkerLockUnlock(cache TokenCache, app App) {
	token := cache.LockAndRead(app)
	cache.SaveAndUnlock(app, token)
}

// -----------------------------------------------------------
// Benchmarks
// -----------------------------------------------------------
var numApps = 20
var numWorkers = 10

func Benchmark_SingleMutex_Get(b *testing.B) {
	cache := TokenCacheSingleMutex{cache: make(map[App]Token)}
	tester := StartNewTester(numApps, numWorkers, cache, WorkerGet)
	tester.RunBenchmark(b)
}

// FIXME: EXPLODES
//func Benchmark_SingleMutex_LockUnlock(b *testing.B) {
//	cache := TokenCacheSingleMutex{cache: make(map[App]Token)}
//	tester := StartNewTester(numApps, numWorkers, cache, WorkerLockUnlock)
//	tester.RunBenchmark(b)
//}

func Benchmark_MutexPerKey_Get(b *testing.B) {
	cache := TokenCacheMutexPerKey{cache: make(map[App]LockableToken)}
	tester := StartNewTester(numApps, numWorkers, cache, WorkerGet)
	tester.RunBenchmark(b)
}

// FIXME: EXPLODES
//func Benchmark_MutexPerKey_LockUnlock(b *testing.B) {
//	cache := TokenCacheMutexPerKey{cache: make(map[App]LockableToken)}
//	tester := StartNewTester(numApps, numWorkers, cache, WorkerLockUnlock)
//	tester.RunBenchmark(b)
//}

func Benchmark_Channel_Get(b *testing.B) {
	cache := TokenCacheChannel{cache: make(map[App]chan Token)}
	tester := StartNewTester(numApps, numWorkers, cache, WorkerGet)
	tester.RunBenchmark(b)
}

func Benchmark_Channel_LockUnlock(b *testing.B) {
	cache := TokenCacheChannel{cache: make(map[App]chan Token)}
	tester := StartNewTester(numApps, numWorkers, cache, WorkerLockUnlock)
	tester.RunBenchmark(b)
}
