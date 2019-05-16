package rabbitevent

import (
	"errors"
	"net"
)

var (
	ErrExchangeNameEmpty = errors.New("exchange name can not be empty") // exchange缺少名称
	ErrExchangeKindEmpty = errors.New("exchange kind can not be empty") // exchange缺少名称
	ErrQueueNameEmpty    = errors.New("queue name can not be empty")    // queue缺少名称
	ErrNoExchangeConfig  = errors.New("exchange config can not be nil") // exchange配置
	ErrNoPublisherConfig = errors.New("publisher config can not be nil")
)

// 检验exchange配置
func validateExchangeConfig(config ExchangeConfig) error {

	// 校验exchange 配置
	if config.Name == "" {
		return ErrExchangeNameEmpty
	}
	if config.Kind == "" {
		return ErrExchangeKindEmpty
	}

	return nil
}

// 检验queue配置
func validateQueueConfig(config QueueConfig) error {
	if config.Name == "" {
		return ErrQueueNameEmpty
	}
	return nil
}

func localIp() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "unknown"
	}
	for _, address := range addrs {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}

	return "unknown"
}
