package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	apiv1 "github.com/acorn-io/acorn/pkg/apis/api.acorn.io/v1"
	v1 "github.com/acorn-io/acorn/pkg/apis/internal.acorn.io/v1"
	"github.com/acorn-io/acorn/pkg/autoupgrade"
	"github.com/acorn-io/acorn/pkg/build"
	cli "github.com/acorn-io/acorn/pkg/cli/builder"
	"github.com/acorn-io/acorn/pkg/client"
	"github.com/acorn-io/acorn/pkg/deployargs"
	"github.com/acorn-io/acorn/pkg/dev"
	"github.com/acorn-io/acorn/pkg/rulerequest"
	"github.com/acorn-io/acorn/pkg/tags"
	"github.com/acorn-io/acorn/pkg/wait"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/yaml"
)

func NewRun(out io.Writer) *cobra.Command {
	cmd := cli.Command(&Run{out: out}, cobra.Command{
		Use:          "run [flags] IMAGE|DIRECTORY [acorn args]",
		SilenceUsage: true,
		Short:        "Run an app from an image or Acornfile",
		Example: `# Publish and Expose Port Syntax
  # Publish port 80 for any containers that define it as a port
  acorn run -p 80 .

  # Publish container "myapp" using the hostname app.example.com
  acorn run --publish app.example.com:myapp .

  # Expose port 80 to the rest of the cluster as port 8080
  acorn run --expose 8080:80/http .

# Labels and Annotations Syntax
  # Add a label to all resources created by the app
  acorn run --label key=value .

  # Add a label to resources created for all containers
  acorn run --label containers:key=value .

  # Add a label to the resources created for the volume named "myvolume"
  acorn run --label volumes:myvolume:key=value .

# Link Syntax
  # Link the running acorn application named "mydatabase" into the current app, replacing the container named "db"
  acorn run --link mydatabase:db .

# Secret Syntax
  # Bind the acorn secret named "mycredentials" into the current app, replacing the secret named "creds". See "acorn secrets --help" for more info
  acorn run --secret mycredentials:creds .

# Volume Syntax
  # Create the volume named "mydata" with a size of 5 gigabyes and using the "fast" storage class
  acorn run --volume mydata,size=5G,class=fast .

  # Bind the acorn volume named "mydata" into the current app, replacing the volume named "data", See "acorn volumes --help for more info"
  acorn run --volume mydata:data .

# Automatic upgrades
  # Automatic upgrade for an app will be enabled if '#', '*', or '**' appears in the image's tag. Tags will sorted according to the rules for these special characters described below. The newest tag will be selected for upgrade.

  # '#' denotes a segment of the image tag that should be sorted numerically when finding the newest tag.
  # This example deploys the hello-world app with auto-upgrade enabled and matching all major, minor, and patch versions:
  acorn run myorg/hello-world:v#.#.#

  # '*' denotes a segment of the image tag that should sorted alphabetically when finding the latest tag.
  # In this example, if you had a tag named alpha and a tag named zeta, zeta would be recognized as the newest:
  acorn run myorg/hello-world:*

  # '**' denotes a wildcard. This segment of the image tag won't be considered when sorting. This is useful if your tags have a segment that is unpredictable.
  # This example would sort numerically according to major and minor version (ie v1.2) and ignore anything following the "-":
  acorn run myorg/hello-world:v#.#-**

  # Automatic upgrades can be configured explicitly via a flag.
  # In this example, the tag will always be "latest", but acorn will periodically check to see if new content has been pushed to that tag:
  acorn run --auto-upgrade enabled myorg/hello-world:latest`})
	cmd.PersistentFlags().Lookup("dangerous").Hidden = true
	cmd.Flags().SetInterspersed(false)
	return cmd
}

type Run struct {
	RunArgs
	Interactive       bool  `usage:"Enable interactive dev mode: build image, stream logs/status in the foreground and stop on exit" short:"i" name:"dev"`
	BidirectionalSync bool  `usage:"In interactive mode download changes in addition to uploading" short:"b"`
	Wait              *bool `usage:"Wait for app to become ready before command exiting (default true)"`
	Quiet             bool  `usage:"Do not print status" short:"q"`

	out io.Writer
}

type RunArgs struct {
	Name                string   `usage:"Name of app to create" short:"n"`
	File                string   `short:"f" usage:"Name of the build file" default:"DIRECTORY/Acornfile"`
	Volume              []string `usage:"Bind an existing volume (format existing:vol-name,field=value) (ex: pvc-name:app-data)" short:"v" split:"false"`
	Secret              []string `usage:"Bind an existing secret (format existing:sec-name) (ex: sec-name:app-secret)" short:"s"`
	Link                []string `usage:"Link external app as a service in the current app (format app-name:container-name)"`
	PublishAll          *bool    `usage:"Publish all (true) or none (false) of the defined ports of application" short:"P"`
	Publish             []string `usage:"Publish port of application (format [public:]private) (ex 81:80)" short:"p"`
	Expose              []string `usage:"In cluster expose ports of an application (format [public:]private) (ex 81:80)"`
	Profile             []string `usage:"Profile to assign default values"`
	Env                 []string `usage:"Environment variables to set on running containers" short:"e"`
	Label               []string `usage:"Add labels to the app and the resources it creates (format [type:][name:]key=value) (ex k=v, containers:k=v)" short:"l"`
	Annotation          []string `usage:"Add annotations to the app and the resources it creates (format [type:][name:]key=value) (ex k=v, containers:k=v)"`
	Dangerous           bool     `usage:"Automatically approve all privileges requested by the application"`
	Output              string   `usage:"Output API request without creating app (json, yaml)" short:"o"`
	TargetNamespace     string   `usage:"The name of the namespace to be created and deleted for the application resources"`
	AutoUpgrade         string   `usage:"Enabled automatic upgrades. Values: enabled, notify, disabled (default). Notify will flag apps as having upgrades available in the output of acorn ps"`
	AutoUpgradeInterval string   `usage:"When auto-upgrade is enabled, this is the interval at which to check for new releases"`
}

func (s RunArgs) ToOpts() (client.AppRunOptions, error) {
	var (
		opts client.AppRunOptions
		err  error
	)

	opts.Name = s.Name
	opts.Profiles = s.Profile
	opts.TargetNamespace = s.TargetNamespace
	opts.AutoUpgrade = s.AutoUpgrade
	opts.AutoUpgradeInterval = s.AutoUpgradeInterval

	opts.AutoUpgrade, err = parseAutoUpgrade(s.AutoUpgrade)
	if err != nil {
		return opts, err
	}

	opts.Volumes, err = v1.ParseVolumes(s.Volume, true)
	if err != nil {
		return opts, err
	}

	opts.Secrets, err = v1.ParseSecrets(s.Secret)
	if err != nil {
		return opts, err
	}

	opts.Links, err = v1.ParseLinks(s.Link)
	if err != nil {
		return opts, err
	}

	opts.Env = v1.ParseNameValues(true, s.Env...)

	opts.Labels, err = v1.ParseScopedLabels(s.Label...)
	if err != nil {
		return opts, err
	}

	opts.Annotations, err = v1.ParseScopedLabels(s.Annotation...)
	if err != nil {
		return opts, err
	}

	opts.Ports, err = v1.ParsePortBindings(true, s.Publish)
	if err != nil {
		return opts, err
	}

	expose, err := v1.ParsePortBindings(false, s.Expose)
	if err != nil {
		return opts, err
	}
	opts.Ports = append(opts.Ports, expose...)

	if s.PublishAll != nil && *s.PublishAll {
		opts.PublishMode = v1.PublishModeAll
	} else if s.PublishAll != nil && !*s.PublishAll {
		opts.PublishMode = v1.PublishModeNone
	}

	return opts, nil
}

func isDirectory(cwd string) (bool, error) {
	if s, err := os.Stat(cwd); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	} else if !s.IsDir() {
		return false, fmt.Errorf("%s is not a directory", cwd)
	}
	return true, nil
}

func parseAutoUpgrade(val string) (string, error) {
	if val == "" {
		return "", nil
	}
	if strings.EqualFold("enabled", val) || strings.EqualFold("true", val) {
		return "enabled", nil
	}
	if strings.EqualFold("notify", val) {
		return "notify", nil
	}
	if strings.EqualFold("disabled", val) || strings.EqualFold("false", val) {
		return "disabled", nil
	}
	return "", fmt.Errorf("unsupported auto-upgrade value: %v", val)
}

func buildImage(ctx context.Context, file, cwd string, args, profiles []string) (string, error) {
	params, err := build.ParseParams(file, cwd, args)
	if err != nil {
		return "", err
	}

	image, err := build.Build(ctx, file, &build.Options{
		Cwd:      cwd,
		Args:     params,
		Profiles: profiles,
	})
	if err != nil {
		return "", err
	}

	return image.ID, nil
}

func (s *Run) Run(cmd *cobra.Command, args []string) error {
	c, err := client.Default()
	if err != nil {
		return err
	}

	// Force install prompt if needed
	_, err = c.Info(cmd.Context())
	if err != nil {
		return err
	}

	opts, err := s.ToOpts()
	if err != nil {
		return err
	}

	cwd := "."
	if len(args) > 0 {
		cwd = args[0]
	}

	isDir, err := isDirectory(cwd)
	if err != nil {
		return err
	}

	if s.Interactive && isDir {

		return dev.Dev(cmd.Context(), s.File, &dev.Options{
			Args:   args,
			Client: c,
			Build: build.Options{
				Cwd:      cwd,
				Profiles: opts.Profiles,
			},
			Run:               opts,
			Dangerous:         s.Dangerous,
			BidirectionalSync: s.BidirectionalSync,
		})
	}

	if s.Interactive {
		s.Profile = append([]string{"dev?"}, s.Profile...)
	}

	image := cwd
	if isDir {
		image, err = buildImage(cmd.Context(), s.File, cwd, args, s.Profile)
		if err == pflag.ErrHelp {
			return nil
		} else if err != nil {
			return err
		}
	}

	image, err = resolve(cmd.Context(), c, image)
	if err != nil {
		return err
	}

	if len(args) > 1 {
		_, flags, err := deployargs.ToFlagsFromImage(cmd.Context(), c, image)
		if err != nil {
			return err
		}

		deployParams, err := flags.Parse(args)
		if pflag.ErrHelp == err {
			return nil
		} else if err != nil {
			return err
		}

		opts.DeployArgs = deployParams
	}

	if s.Output != "" {
		app := client.ToApp(c.GetNamespace(), image, &opts)
		return outputApp(s.out, s.Output, app)
	}

	app, err := rulerequest.PromptRun(cmd.Context(), c, s.Dangerous, image, opts)
	if err != nil {
		return err
	}

	fmt.Println(app.Name)

	if s.Interactive {
		go func() { _ = dev.LogLoop(cmd.Context(), c, app, nil) }()
		go func() { _ = dev.AppStatusLoop(cmd.Context(), c, app) }()
		<-cmd.Context().Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.AppStop(ctx, app.Name)
	} else if s.Wait == nil || *s.Wait {
		return wait.App(cmd.Context(), c, app.Name, s.Quiet)
	}

	return nil
}

func outputApp(out io.Writer, format string, app *apiv1.App) error {
	data, err := json.Marshal(app)
	if err != nil {
		return err
	}

	mapData := map[string]any{}
	if err := json.Unmarshal(data, &mapData); err != nil {
		return err
	}

	delete(mapData, "status")
	delete(mapData["metadata"].(map[string]any), "uid")
	delete(mapData["metadata"].(map[string]any), "resourceVersion")
	delete(mapData["metadata"].(map[string]any), "managedFields")
	delete(mapData["metadata"].(map[string]any), "creationTimestamp")

	if format == "json" {
		data, err = json.MarshalIndent(mapData, "", "  ")
	} else {
		data, err = yaml.Marshal(mapData)
	}
	if err != nil {
		return err
	}

	if out == nil {
		_, err = os.Stdout.Write(data)
	} else {
		_, err = out.Write(data)
	}
	return err
}

func resolve(ctx context.Context, c client.Client, image string) (string, error) {
	if _, pattern := autoupgrade.AutoUpgradePattern(image); pattern {
		return image, nil
	}
	localImage, err := c.ImageGet(ctx, image)
	if apierrors.IsNotFound(err) {
		if tags.IsLocalReference(image) {
			return "", err
		}
	} else if err != nil {
		return "", err
	} else {
		return strings.TrimPrefix(localImage.Digest, "sha256:"), nil
	}
	return image, nil
}
