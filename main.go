package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// зберігає результат опитування адреси
type CheckResult struct {
	URL        string
	StatusCode int
	StatusText string
	Duration   time.Duration
	Err        error
}

// інтерфейс для перевірки ресурсів
type Checker interface {
	Check(url string) CheckResult
}

// структура нашого HTTP-клієнта
type HTTPChecker struct {
	client *http.Client
}

// створює екземпляр клієнта із заданим таймаутом
func NewHTTPChecker(timeout time.Duration) *HTTPChecker {
	return &HTTPChecker{
		client: &http.Client{Timeout: timeout},
	}
}

// виконує запит і заміряє час
func (c *HTTPChecker) Check(url string) CheckResult {
	start := time.Now()
	resp, err := c.client.Get(url)
	duration := time.Since(start)

	if err != nil {
		return CheckResult{
			URL:      url,
			Duration: duration,
			Err:      err,
		}
	}
	defer resp.Body.Close()

	return CheckResult{
		URL:        url,
		StatusCode: resp.StatusCode,
		StatusText: resp.Status,
		Duration:   duration,
	}
}

// допоміжна функція для заміру послідовного часу
func measureSequential(checker Checker, urls []string) time.Duration {
	start := time.Now()
	for _, url := range urls {
		_ = checker.Check(url)
	}
	return time.Since(start)
}

func main() {
	urls := []string{
		"https://go.dev",
		"https://github.com",
		"https://httpbin.org/status/404",
		"https://invalid-fake-host-test.org",
	}

	var checker Checker = NewHTTPChecker(5 * time.Second)

	// заміряємо час послідовного проходу для порівняння
	seqDuration := measureSequential(checker, urls)

	// запускаємо конкурентну перевірку через канали (Рівень 3)
	fmt.Println("HTTP URL Health Checker")
	fmt.Println("Конкурентна перевірка адрес")

	startConc := time.Now()
	var wg sync.WaitGroup

	// створюємо буферизований канал на кількість наших адрес
	resultsChan := make(chan CheckResult, len(urls))

	for _, url := range urls {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			// Замість Mutex просто відправляємо результат у канал
			resultsChan <- checker.Check(u)
		}(url)
	}

	// окрема горутина-координатор: чекає завершення всіх запитів і закриває канал
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	var total, success, failed int

	// головний потік вичитує результати з каналу, доки координатор його не закриє
	for res := range resultsChan {
		total++
		if res.Err != nil {
			failed++
			fmt.Printf("-> %s: Помилка (%v)\n", res.URL, res.Err)
		} else {
			success++
			fmt.Printf("-> %s: Status %s (час: %v)\n", res.URL, res.StatusText, res.Duration.Round(time.Millisecond))
		}
	}

	concDuration := time.Since(startConc)

	fmt.Println("\nПІДСУМКОВИЙ ЗВІТ МОНІТОРИНГУ:")
	fmt.Printf("Усього перевірено адрес: %d\n", total)
	fmt.Printf("Успішних з'єднань: %d\n", success)
	fmt.Printf("Мережевих помилок: %d\n", failed)
	fmt.Printf("Загальний час роботи: %v (проти %v у послідовному режимі)\n",
		concDuration.Round(time.Millisecond),
		seqDuration.Round(time.Millisecond),
	)
}
