# Redshift Plugin

Send events to AWS Redshift on ingestion.

## Instructions

### 1. [Create a Redshift Cluster](https://docs.aws.amazon.com/redshift/latest/dg/tutorial-loading-data-launch-cluster.html)

### 2. Make sure PostHog can access your cluster

This might require a few things:

-   [Allowing public access to the cluster](https://aws.amazon.com/premiumsupport/knowledge-center/redshift-cluster-private-public/)
-   [Ensuring your VPC security group allows traffic to and from the Redshift cluster](https://docs.aws.amazon.com/vpc/latest/userguide/VPC_SecurityGroups.html) - If this is not possible in your case, you should consider using our [S3 plugin](https://posthog.com/plugins/s3-export) and then setting up your own system for getting data into your Redshift cluster

### 3. Create a user with table creation priviledges

We need to create a new table to store events and execute `INSERT` queries. You can and should block us from doing anything else on any other tables. Giving us table creation permissions should be enough to ensure this:

```sql
CREATE USER posthog WITH PASSWORD '123456yZ';
GRANT CREATE ON DATABASE your_database TO posthog;
```

### 4. Add the connection details at the plugin configuration step in PostHog
