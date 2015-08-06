================
Releasing Tephra
================

This page describes the step-by-step process of how to perform an official Tephra version release,
including deploying the release artifacts to Maven repositories and the additional administrative
steps to complete the release process.

Prerequisites
=============

Maven Settings File
-------------------

Prior to performing a Tephra release, you must have an entry such as this in your
``~/.m2/settings.xml`` file to authenticate when deploying the release artifacts::

  <?xml version="1.0" encoding="UTF-8"?>
  <settings xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd"
      xmlns="http://maven.apache.org/SETTINGS/1.0.0"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <servers>
      <server>
        <id>sonatype.snapshots</id>
        <username>USERNAME</username>
        <password>PASSWORD</password>
      </server>
      <server>
        <id>sonatype.release</id>
        <username>USERNAME</username>
        <password>PASSWORD</password>
      </server>
    </servers>
  </settings>
  
Replace ``USERNAME`` and ``PASSWORD`` with the correct values for your user account.  See the
`Maven Encryption Guide <http://maven.apache.org/guides/mini/guide-encryption.html>`_ for details
on how to avoid storing the plaintext password in the settings.xml file.

PGP Key
-------

You will also need to have created a PGP (or GPG) key pair, which will be used in signing the release
artifacts.  For more information on using the Maven GPG plugin, see this `introduction
<http://blog.sonatype.com/2010/01/how-to-generate-pgp-signatures-with-maven/>`_ from Sonatype and
the Maven GPG Plugin `usage page
<https://maven.apache.org/plugins/maven-gpg-plugin/usage.html>`_.  You may also want to run
gpg-agent in order to avoid being prompted multiple times for the GPG key passphrase when
performing a release.


Performing the Release
======================

Ensure Local Branch is Up-to-date
---------------------------------

First, make sure your local copy of the ``develop`` branch is up-to-date with all changes::

  git checkout develop
  git pull

Create the Release Branch
-------------------------

Next, create a release branch from ``develop``::

  git checkout -b release/N.N.N

replacing ``N.N.N`` with the desired release version.

Prepare the Release
-------------------

While on the release branch, prepare the release::
  
  mvn clean release:prepare -P release
  
This will prompt you for the release version and the git tag to use for the release.  By
convention, we use ``vN.N.N`` for the release tag (ie. v0.6.0 for release 0.6.0).

Perform the Release
-------------------

Perform the release by running::
  
  mvn release:perform -P release

This will checkout the source code using the release tag, build the release and deploy it to the
oss.sonatype.org repository.

Release the Staging Repository in Artifactory
---------------------------------------------

Release the artifact bundle in Artifactory:

1. Login to https://oss.sonatype.org (you will need to use the same credentials you have
   configured in your ``~/.m2/settings.xml`` file).
2. Go to "Staging Repos".
3. Find the "comcontinuuity" repo with the Tephra release.  Be sure to expand the contents of the
   repo to confirm that it contains the correct Tephra artifacts. 
4. Click on the "Release" button at top, and enter a brief description, such as "Tephra N.N.N
   release".

Update Git Branches
-------------------

After the release is complete, update the other git branches with the release changes::
  
  git checkout master
  git merge release/N.N.N
  git checkout develop
  git merge master


Announcing and Completing the Release
=====================================

Mark the release as complete in JIRA (in the Tephra Administration section):

1. Add a release for the next version, if necessary
2. Set a release date and release the released version

Create a new release in github:

1. Create a new release based on the release tag used
2. Add the release description
3. Upload release artifacts from ``tephra-distribution/target/``

Finally, announce the release on the Tephra mailing lists: tephra-user@googlegroups.com, tephra-dev@googlegroups.com
