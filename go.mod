module github.com/creachadair/ffstools

go 1.23

toolchain go1.23.1

require (
	github.com/creachadair/atomicfile v0.3.5
	github.com/creachadair/badgerstore v0.5.1
	github.com/creachadair/bitcaskstore v0.0.0-20240929224447-ee17f3bd6a35
	github.com/creachadair/boltstore v0.0.0-20241005210321-efa554c3757f
	github.com/creachadair/chirp v0.0.0-20241006224827-23ecec328c5d
	github.com/creachadair/chirpstore v0.0.0-20241006224935-4ff549c78ea6
	github.com/creachadair/command v0.1.17
	github.com/creachadair/ffs v0.6.6
	github.com/creachadair/ffuse v0.0.0-20241006225456-478c71db425a
	github.com/creachadair/flax v0.0.1
	github.com/creachadair/gcsstore v0.0.0-20241006224658-6404a11840e7
	github.com/creachadair/getpass v0.2.1
	github.com/creachadair/keyfile v0.8.3
	github.com/creachadair/leveldbstore v0.0.0-20240929224719-7595f7b3ff19
	github.com/creachadair/mds v0.21.2
	github.com/creachadair/pebblestore v0.0.0-20240929224607-336b4dad2057
	github.com/creachadair/pogrebstore v0.0.0-20240929224651-91f519295e4d
	github.com/creachadair/s3store v0.0.0-20241006225107-d3045dd87f1e
	github.com/creachadair/sqlitestore v0.0.0-20240929224644-2cda36af8973
	github.com/creachadair/taskgroup v0.11.0
	github.com/hanwen/go-fuse/v2 v2.6.1
	github.com/klauspost/compress v1.17.10
	github.com/pkg/xattr v0.4.10
	golang.org/x/crypto v0.28.0
	google.golang.org/protobuf v1.34.2
	gopkg.in/yaml.v3 v3.0.1
)

require (
	cel.dev/expr v0.16.2 // indirect
	cloud.google.com/go v0.115.1 // indirect
	cloud.google.com/go/auth v0.9.7 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.4 // indirect
	cloud.google.com/go/compute/metadata v0.5.2 // indirect
	cloud.google.com/go/iam v1.2.1 // indirect
	cloud.google.com/go/monitoring v1.21.1 // indirect
	cloud.google.com/go/storage v1.44.0 // indirect
	git.mills.io/prologic/bitcask v1.0.2 // indirect
	github.com/DataDog/zstd v1.5.6 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.24.2 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.48.2 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.48.2 // indirect
	github.com/abcum/lcp v0.0.0-20201209214815-7a3f3840be81 // indirect
	github.com/akrylysov/pogreb v0.10.2 // indirect
	github.com/aws/aws-sdk-go-v2 v1.32.0 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.6 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.27.41 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.17.39 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.15 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.19 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.19 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.1 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.4.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.18.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.65.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.24.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.28.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.32.0 // indirect
	github.com/aws/smithy-go v1.22.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/census-instrumentation/opencensus-proto v0.4.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cncf/xds/go v0.0.0-20240905190251-b4127c9b8d78 // indirect
	github.com/cockroachdb/errors v1.11.3 // indirect
	github.com/cockroachdb/fifo v0.0.0-20240816210425-c5d0cb0b6fc0 // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/pebble v1.1.2 // indirect
	github.com/cockroachdb/redact v1.1.5 // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20230807174530-cc333fc44b06 // indirect
	github.com/creachadair/msync v0.3.1 // indirect
	github.com/dgraph-io/badger/v4 v4.3.1 // indirect
	github.com/dgraph-io/ristretto v1.0.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/envoyproxy/go-control-plane v0.13.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.1.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/getsentry/sentry-go v0.29.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gofrs/flock v0.12.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers/go v0.0.0-20230110200425-62e4d2e5b215 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/s2a-go v0.1.8 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.4 // indirect
	github.com/googleapis/gax-go/v2 v2.13.0 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/ncruces/go-strftime v0.1.9 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/plar/go-adaptive-radix-tree v1.0.5 // indirect
	github.com/prometheus/client_golang v1.20.4 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.60.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/syndtr/goleveldb v1.0.0 // indirect
	go.etcd.io/bbolt v1.3.11 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.30.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.55.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.55.0 // indirect
	go.opentelemetry.io/otel v1.30.0 // indirect
	go.opentelemetry.io/otel/metric v1.30.0 // indirect
	go.opentelemetry.io/otel/sdk v1.30.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.30.0 // indirect
	go.opentelemetry.io/otel/trace v1.30.0 // indirect
	golang.org/x/exp v0.0.0-20241004190924-225e2abe05e6 // indirect
	golang.org/x/net v0.30.0 // indirect
	golang.org/x/oauth2 v0.23.0 // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/sys v0.26.0 // indirect
	golang.org/x/term v0.25.0 // indirect
	golang.org/x/text v0.19.0 // indirect
	golang.org/x/time v0.7.0 // indirect
	google.golang.org/api v0.199.0 // indirect
	google.golang.org/genproto v0.0.0-20240930140551-af27646dc61f // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240930140551-af27646dc61f // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240930140551-af27646dc61f // indirect
	google.golang.org/grpc v1.67.1 // indirect
	google.golang.org/grpc/stats/opentelemetry v0.0.0-20241004113128-859602c14c6c // indirect
	modernc.org/gc/v3 v3.0.0-20241004144649-1aea3fae8852 // indirect
	modernc.org/libc v1.61.0 // indirect
	modernc.org/mathutil v1.6.0 // indirect
	modernc.org/memory v1.8.0 // indirect
	modernc.org/sqlite v1.33.1 // indirect
	modernc.org/strutil v1.2.0 // indirect
	modernc.org/token v1.1.0 // indirect
)

retract v0.4.1
