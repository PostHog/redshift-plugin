# WIP Redshift Plugin

Send events to AWS Redshift.

## Instructions 

> Note: This is still under development.

1. Create cluster
2. Allow public access
3. Make sure your security group configuration allows connections from any IP (use S3 as an intermediary if you can't do this)
4. Create a user with table creation priviledges:

```sql
CREATE USER posthog WITH PASSWORD '123456yZ';
GRANT CREATE ON DATABASE your_database TO posthog;
```

5. Add details to plugin config

