package main

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/util/strutil"
)

const (
	rdsLabel              = model.MetaLabelPrefix + "rds_"
	rdsLabelAZ            = rdsLabel + "availability_zone"
	rdsLabelInstanceID    = rdsLabel + "instance_id"
	rdsLabelInstanceState = rdsLabel + "instance_state"
	rdsLabelInstanceType  = rdsLabel + "instance_type"
	rdsLabelEngine        = rdsLabel + "engine"
	rdsLabelEngineVersion = rdsLabel + "engine_version"
	rdsLabelTag           = rdsLabel + "tag_"
	rdsLabelVPCID         = rdsLabel + "vpc_id"
	rdsLabelCluster       = rdsLabel + "cluster"
	rdsLabelInstanceRole  = rdsLabel + "role"
	rdsLabelRegion        = rdsLabel + "region"
)

type discovery struct {
	refreshInterval int
	logger          log.Logger
	filters         []*rds.Filter
}

func newDiscovery(conf sdConfig, logger log.Logger) (*discovery, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	d := &discovery{
		logger:          logger,
		refreshInterval: conf.RefreshInterval,
		filters:         conf.Filters,
	}

	return d, nil
}

func (d *discovery) Run(ctx context.Context, ch chan<- []*targetgroup.Group) {
    for c := time.Tick(time.Duration(d.refreshInterval) * time.Second); ; {
        var tgs []*targetgroup.Group

        sess := session.Must(session.NewSession())
        client := rds.New(sess)

        clusterWriterMap := make(map[string]bool)

	// Cluster
        err := client.DescribeDBClustersPagesWithContext(ctx, &rds.DescribeDBClustersInput{},
            func(page *rds.DescribeDBClustersOutput, lastPage bool) bool {
                for _, cluster := range page.DBClusters {
                    for _, member := range cluster.DBClusterMembers {
                        if member.IsClusterWriter != nil && *member.IsClusterWriter {
                            clusterWriterMap[*member.DBInstanceIdentifier] = true
                        }
                    }
                }
                return !lastPage
            },
        )
        if err != nil {
            level.Error(d.logger).Log("msg", "could not describe db clusters", "err", err)
            continue
        }

        input := &rds.DescribeDBInstancesInput{
            Filters: d.filters,
        }
	// Instance
	if err := client.DescribeDBInstancesPagesWithContext(ctx, input, func(out *rds.DescribeDBInstancesOutput, lastPage bool) bool {
            for _, dbi := range out.DBInstances {

		labels := model.LabelSet{}

		if dbi.DBInstanceIdentifier != nil {
			labels[rdsLabelInstanceID] = model.LabelValue(*dbi.DBInstanceIdentifier)
		}

		if dbi.AvailabilityZone != nil {
			labels[rdsLabelAZ] = model.LabelValue(*dbi.AvailabilityZone)
		}
		
		if dbi.DBInstanceStatus != nil {
			labels[rdsLabelInstanceState] = model.LabelValue(*dbi.DBInstanceStatus)
		}

		if dbi.DBInstanceClass != nil {
			labels[rdsLabelInstanceType] = model.LabelValue(*dbi.DBInstanceClass)	
		}

		if dbi.Endpoint != nil && dbi.Endpoint.Address != nil && dbi.Endpoint.Port != nil {
			addr := net.JoinHostPort(*dbi.Endpoint.Address, strconv.FormatInt(*dbi.Endpoint.Port, 10))
			labels[model.AddressLabel] = model.LabelValue(addr)	
		}

		if dbi.Engine != nil {
        		labels[rdsLabelEngine] = model.LabelValue(*dbi.Engine)
    		}

		if dbi.EngineVersion != nil {
        		labels[rdsLabelEngineVersion] = model.LabelValue(*dbi.EngineVersion)
    		}

		if dbi.DBSubnetGroup != nil && dbi.DBSubnetGroup.VpcId != nil {
        		labels[rdsLabelVPCID] = model.LabelValue(*dbi.DBSubnetGroup.VpcId)
    		}		

		if dbi.DBClusterIdentifier != nil {
    			labels[rdsLabelCluster] = model.LabelValue(*dbi.DBClusterIdentifier)
		}

                if _, ok := clusterWriterMap[*dbi.DBInstanceIdentifier]; ok {
                    labels[rdsLabel + "is_cluster_writer"] = model.LabelValue("true")
                } else {
                    labels[rdsLabel + "is_cluster_writer"] = model.LabelValue("false")
                }

		if dbi.AvailabilityZone != nil {
			labels[rdsLabelRegion] = model.LabelValue(*dbi.AvailabilityZone)[:len(model.LabelValue(*dbi.AvailabilityZone))-1]
		}

		tags, err := listTagsForInstance(client, dbi)
		if err != nil {
			level.Error(d.logger).Log("msg", "could not list tags for db instance", "err", err)
		}
		    
    		if err == nil && tags != nil && tags.TagList != nil {
        		for _, t := range tags.TagList {
            			if t.Key != nil && t.Value != nil {
                			name := strutil.SanitizeLabelName(*t.Key)
                			labels[rdsLabelTag+model.LabelName(name)] = model.LabelValue(*t.Value)
            			}
        		}
    		}

                tgs = append(tgs, &targetgroup.Group{
                    Source:  *dbi.DBInstanceIdentifier,
                    Targets: []model.LabelSet{{model.AddressLabel: labels[model.AddressLabel]}},
                    Labels:  labels,
                })
            }
            return true
        }); err != nil {
            	level.Error(d.logger).Log("msg", "could not describe db instances", "err", err)
		time.Sleep(time.Duration(d.refreshInterval) * time.Second)
            	continue
        }

        ch <- tgs

        select {
        case <-c:
            continue
        case <-ctx.Done():
            return
        }
    }
}


func listTagsForInstance(client *rds.RDS, dbi *rds.DBInstance) (*rds.ListTagsForResourceOutput, error) {
	input := &rds.ListTagsForResourceInput{
		ResourceName: aws.String(*dbi.DBInstanceArn),
	}
	return client.ListTagsForResource(input)
}
