package controller

import (
	"context"
	"time"

	v1 "github.com/acorn-io/acorn/pkg/apis/internal.acorn.io/v1"
	"github.com/acorn-io/acorn/pkg/autoupgrade"
	"github.com/acorn-io/acorn/pkg/crds"
	"github.com/acorn-io/acorn/pkg/dns"
	"github.com/acorn-io/acorn/pkg/k8sclient"
	"github.com/acorn-io/acorn/pkg/scheme"
	"github.com/acorn-io/baaah"
	"github.com/acorn-io/baaah/pkg/apply"
	"github.com/acorn-io/baaah/pkg/restconfig"
	"github.com/acorn-io/baaah/pkg/router"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	dnsRenewPeriodHours = 24 * time.Hour
)

type Controller struct {
	Router *router.Router
	client client.Client
	Scheme *runtime.Scheme
	apply  apply.Apply
}

func New() (*Controller, error) {
	router, err := baaah.DefaultRouter(scheme.Scheme)
	if err != nil {
		return nil, err
	}

	cfg, err := restconfig.New(scheme.Scheme)
	if err != nil {
		return nil, err
	}

	client, err := k8sclient.New(cfg)
	if err != nil {
		return nil, err
	}

	apply := apply.New(client)

	routes(router)

	return &Controller{
		Router: router,
		client: client,
		Scheme: scheme.Scheme,
		apply:  apply,
	}, nil
}

func (c *Controller) Start(ctx context.Context) error {
	if err := crds.Create(ctx, c.Scheme, v1.SchemeGroupVersion); err != nil {
		return err
	}
	if err := c.initData(ctx); err != nil {
		return err
	}

	dnsInit := dns.NewDaemon(c.client)
	go wait.UntilWithContext(ctx, dnsInit.RenewAndSync, dnsRenewPeriodHours)

	autoUpgrade := autoupgrade.NewDaemon(c.client)
	go autoUpgrade.StartSync(ctx)

	return c.Router.Start(ctx)
}
