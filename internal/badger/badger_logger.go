package badger

import "github.com/rs/zerolog/log"

// Wrap our zerolog with the Logger interface badger has exposed.

type badgerLogger struct{}

func (b badgerLogger) Errorf(fmt string, v ...interface{}) {
	log.Error().Msgf(fmt, v...)
}

func (b badgerLogger) Warningf(fmt string, v ...interface{}) {
	log.Warn().Msgf(fmt, v...)
}

func (b badgerLogger) Infof(fmt string, v ...interface{}) {
	log.Info().Msgf(fmt, v...)
}

func (b badgerLogger) Debugf(fmt string, v ...interface{}) {
	log.Debug().Msgf(fmt, v...)
}
