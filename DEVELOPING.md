# How to publish

## Prerequisite
Ensure you have in your $HOME/.m2/settings.xml the configured userid/password provided by https://central.sonatype.com/
Ensure you have a published Gnu PGP public key (needed for signing artifacts)

# Publish
Use 
    export MAVEN_GPG_PASSPHRASE=....your pgp key pass here ...
    ./mvnw release:prepare -Dresume=false
    ./mvnw release:perform

# References

## How to publish on maven central:
Follow the article here
https://vaadin.com/blog/how-to-publish-java-libraries-to-maven-central-using-your-github-account

## Helpful advices
https://central.sonatype.org/publish/publish-maven/#distribution-management-and-authentication