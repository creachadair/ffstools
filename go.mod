module github.com/creachadair/ffstools

go 1.24

require (
	github.com/creachadair/atomicfile v0.3.8
	github.com/creachadair/badgerstore v0.10.0
	github.com/creachadair/bitcaskstore v0.0.0-20250120175303-c377a553aa6a
	github.com/creachadair/boltstore v0.0.0-20250120175019-69d7c0db9a1e
	github.com/creachadair/buntdbstore v0.0.0-20250120175250-42f8ce8c9298
	github.com/creachadair/chirp v0.0.0-20250613030156-bb9ffd4b9e2a
	github.com/creachadair/chirpstore v0.0.0-20250627004531-f7165ceba2cb
	github.com/creachadair/command v0.1.22
	github.com/creachadair/ffs v0.13.20
	github.com/creachadair/ffuse v0.0.0-20250624214047-8a86f105dd7f
	github.com/creachadair/flax v0.0.5
	github.com/creachadair/gcsstore v0.0.0-20250403223016-1047f431bbd3
	github.com/creachadair/getpass v0.3.0
	github.com/creachadair/keyfile v0.8.4
	github.com/creachadair/leveldbstore v0.0.0-20250120175243-9b22659b15db
	github.com/creachadair/mds v0.25.0
	github.com/creachadair/pebblestore v0.0.0-20250120175134-ae007cbb8e9a
	github.com/creachadair/rosedbstore v0.0.0-20250414170012-195b6796aa88
	github.com/creachadair/s3store v0.0.0-20250514152101-71e9386719c6
	github.com/creachadair/sqlitestore v0.0.0-20250428235354-60d75826f843
	github.com/creachadair/taskgroup v0.13.2
	github.com/fortytw2/leaktest v1.3.0
	github.com/hanwen/go-fuse/v2 v2.8.0
	github.com/klauspost/compress v1.17.11
	github.com/pkg/xattr v0.4.10
	golang.org/x/crypto v0.39.0
	google.golang.org/protobuf v1.36.6
	gopkg.in/yaml.v3 v3.0.1
)

require (
	cel.dev/expr v0.22.1 // indirect
	cloud.google.com/go v0.120.0 // indirect
	cloud.google.com/go/auth v0.15.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute/metadata v0.6.0 // indirect
	cloud.google.com/go/iam v1.4.2 // indirect
	cloud.google.com/go/monitoring v1.24.1 // indirect
	cloud.google.com/go/storage v1.51.0 // indirect
	git.mills.io/prologic/bitcask v1.0.2 // indirect
	github.com/DataDog/zstd v1.5.6 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.27.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.51.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.51.0 // indirect
	github.com/abcum/lcp v0.0.0-20201209214815-7a3f3840be81 // indirect
	github.com/aws/aws-sdk-go-v2 v1.36.3 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.10 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.29.14 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.17.67 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.30 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.34 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.34 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.34 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.7.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.18.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.79.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.25.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.30.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.33.19 // indirect
	github.com/aws/smithy-go v1.22.3 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bwmarrin/snowflake v0.3.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cncf/xds/go v0.0.0-20250121191232-2f005788dc42 // indirect
	github.com/cockroachdb/errors v1.11.3 // indirect
	github.com/cockroachdb/fifo v0.0.0-20240816210425-c5d0cb0b6fc0 // indirect
	github.com/cockroachdb/logtags v0.0.0-20241215232642-bb51bb14a506 // indirect
	github.com/cockroachdb/pebble v1.1.3 // indirect
	github.com/cockroachdb/redact v1.1.5 // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20230807174530-cc333fc44b06 // indirect
	github.com/creachadair/msync v0.5.5 // indirect
	github.com/dgraph-io/badger/v4 v4.5.0 // indirect
	github.com/dgraph-io/ristretto/v2 v2.0.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.32.4 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.2.1 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/getsentry/sentry-go v0.31.1 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gofrs/flock v0.12.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/google/flatbuffers/go v0.0.0-20230110200425-62e4d2e5b215 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.6 // indirect
	github.com/googleapis/gax-go/v2 v2.14.1 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/ncruces/go-strftime v0.1.9 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/plar/go-adaptive-radix-tree v1.0.7 // indirect
	github.com/prometheus/client_golang v1.20.5 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.62.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	github.com/rosedblabs/rosedb/v2 v2.3.9 // indirect
	github.com/rosedblabs/wal v1.3.8 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/syndtr/goleveldb v1.0.0 // indirect
	github.com/tidwall/btree v1.7.0 // indirect
	github.com/tidwall/buntdb v1.3.2 // indirect
	github.com/tidwall/gjson v1.18.0 // indirect
	github.com/tidwall/grect v0.1.4 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tidwall/rtred v0.1.2 // indirect
	github.com/tidwall/tinyqueue v0.1.1 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	go.etcd.io/bbolt v1.3.11 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.35.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.60.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.60.0 // indirect
	go.opentelemetry.io/otel v1.35.0 // indirect
	go.opentelemetry.io/otel/metric v1.35.0 // indirect
	go.opentelemetry.io/otel/sdk v1.35.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.35.0 // indirect
	go.opentelemetry.io/otel/trace v1.35.0 // indirect
	golang.org/x/exp v0.0.0-20250408133849-7e4ce0ab07d0 // indirect
	golang.org/x/exp/typeparams v0.0.0-20250305212735-054e65f0b394 // indirect
	golang.org/x/mod v0.25.0 // indirect
	golang.org/x/net v0.40.0 // indirect
	golang.org/x/oauth2 v0.28.0 // indirect
	golang.org/x/sync v0.15.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/term v0.32.0 // indirect
	golang.org/x/text v0.26.0 // indirect
	golang.org/x/time v0.11.0 // indirect
	golang.org/x/tools v0.33.0 // indirect
	google.golang.org/api v0.227.0 // indirect
	google.golang.org/genproto v0.0.0-20250324211829-b45e905df463 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250324211829-b45e905df463 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250324211829-b45e905df463 // indirect
	google.golang.org/grpc v1.71.0 // indirect
	honnef.co/go/tools v0.6.1 // indirect
	modernc.org/libc v1.64.0 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.10.0 // indirect
	modernc.org/sqlite v1.37.0 // indirect
)

retract (
	v0.11.17 // broken attempt to work around OTEL damage
	v0.10.10 // early return bug in "ffs gc" mark
	[v0.10.8, v0.10.9] // output bug in "ffs root list"
	v0.8.6 // broken go-fuse on macOS
	v0.4.1
)

tool honnef.co/go/tools/staticcheck
