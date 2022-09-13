package appdefinition

import (
	"fmt"

	v1 "github.com/acorn-io/acorn/pkg/apis/internal.acorn.io/v1"
	"github.com/acorn-io/acorn/pkg/autoupgrade"
	"github.com/acorn-io/acorn/pkg/condition"
	"github.com/acorn-io/acorn/pkg/pull"
	"github.com/acorn-io/baaah/pkg/router"
)

func PullAppImage(req router.Request, resp router.Response) error {
	appInstance := req.Object.(*v1.AppInstance)
	cond := condition.Setter(appInstance, resp, v1.AppInstanceConditionPulled)

	mode, on := autoupgrade.AutoUpgradeMode(appInstance.Spec)
	_, isPattern := autoupgrade.AutoUpgradePattern(appInstance.Spec.Image)

	// If the image tag is an auto-upgrade pattern, we need to pull if AvailableAppImage is not blank (indicating
	// there is new image to auto-upgrade to) or if AppImage.ID is blank (indicating the image has never been pulled)
	if isPattern && appInstance.Status.AvailableAppImage == "" && appInstance.Status.AppImage.ID != "" {
		cond.Success()
		return nil
	}

	// If the image tag is not an auto-upgrade pattern, we need to pull if Spec.Image and Status.AppImage.ID are not equal
	// (indicating the image has either never been pulled or the spec image has changed since it was pulled) or
	// if AvailableAppImage is not blank (indicating new content has been pushed to the image tag)
	if !isPattern && appInstance.Spec.Image == appInstance.Status.AppImage.ID && appInstance.Status.AvailableAppImage == "" {
		cond.Success()
		return nil
	}

	// The image tag is a pattern (not a concrete image that can be pulled) and we don't have a concrete image from
	// AvailableAppImage to pull. We have to kick off an auto-upgrade sync and exit
	if on && isPattern && appInstance.Status.AvailableAppImage == "" {
		if mode == "notify" && appInstance.Status.ConfirmUpgradeAppImage != "" {
			cond.Unknown(fmt.Sprintf("confirm upgrade to %v", appInstance.Status.ConfirmUpgradeAppImage))
		} else {
			autoupgrade.Sync()
			cond.Unknown("waiting for image to satisfy auto-upgrade")
		}
		return nil
	}

	desiredImage := appInstance.Spec.Image
	if appInstance.Status.AvailableAppImage != "" {
		desiredImage = appInstance.Status.AvailableAppImage
		appInstance.Status.AvailableAppImage = ""
		appInstance.Status.ConfirmUpgradeAppImage = ""
	}

	appImage, err := pull.AppImage(req.Ctx, req.Client, appInstance.Namespace, desiredImage)
	if err != nil {
		cond.Error(err)
		return nil
	}
	appInstance.Status.AppImage = *appImage

	cond.Success()
	return nil
}
