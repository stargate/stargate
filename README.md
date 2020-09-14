# persistence-extensions

## Building

Before building you will first need to set up your local m2. 

First add this to ~/.m2/settings.xml

```yaml
<servers>
    <server>
        <id>gh</id>
        <username>YOUR_USERNAME</username>
        <password>YOUR_AUTH_TOKEN</password>
    </server>
</servers>
```

- Replace the `YOUR_USERNAME` with your GitHub login name.
- Replace the `YOUR_AUTH_TOKEN` with a generated GitHub personal access token (GitHub > Settings > Developer Settings > Personal access tokens > Generate new token)
  - NOTE: the token needs at least the read:packages scope. Otherwise you will get a Not authorized exception.

Then to build locally run the following:

```sh
./mvnw clean package
``` 
