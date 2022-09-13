package autoupgrade

import (
	"github.com/acorn-io/acorn/pkg/autoupgrade/validate"
	"github.com/acorn-io/acorn/pkg/config"
	"github.com/acorn-io/baaah/pkg/router"
	"github.com/sirupsen/logrus"
)

// HandleAutoUpgradeInterval resets the ticker for auto-upgrade sync interval as it changes in the acorn config
func HandleAutoUpgradeInterval(req router.Request, resp router.Response) error {
	cfg, err := config.Get(req.Ctx, req.Client)
	if err != nil {
		return err
	}

	if cfg.AutoUpgradeInterval != nil {
		err := updateInterval(*cfg.AutoUpgradeInterval)
		return err
	}

	return nil
}

func updateInterval(newInterval string) error {
	if currentInterval == newInterval {
		return nil
	}
	newDur, err := validate.AutoUpgradeInterval(newInterval)
	if err != nil {
		return err
	}

	logrus.Infof("Updating auto-upgrade sync interval to %v", newInterval)
	currentInterval = newInterval
	ticker.Reset(newDur)
	Sync()

	return nil
}
