## Contributing to DataSQRL
Thanks for your interest in DataSQRL. 

# Contributions

DataSQRL welcomes contributions from anyone. Check out the [Developer Documentation](CLAUDE.md)
for more information on the project and the structure of this repository.

Submit a pull request and it will be reviewed by a contributor or committer in the project. The 
contributor may ask for changes or information before being accepted.

## Contributors

Contributors for this project are documented in the project's [CONTRIBUTORS](CONTRIBUTORS.md) file.

# Sign Your Work

The _sign-off_ is a simple line at the end of the message for a commit. All commits need to be signed.
Your signature certifies that you wrote the patch or otherwise have the right to contribute the material
(see [Developer Certificate of Origin](https://developercertificate.org)):

```
This is my commit message

Signed-off-by: John Doe <john.doe@example.com>
```

Git has a [`-s`](https://git-scm.com/docs/git-commit#Documentation/git-commit.txt---signoff) command line option to
append this automatically to your commit message:

```bash
$ git commit -s -m "This is my commit message"
```

Unfortunately, anyone with write access to a repository can easily impersonate another user.
Consider how each commit is associated with a user via their email address.
There's nothing stopping someone from using someone else's email address to make commits.
The issue extends to the signoff message as well.

That's why it's advisable to sign your commits with a unique key. Git offers support for various types of keys,
and this time, we'll walk you through signing your commits using GPG.

#### Setup Git using GPG

Ensure that gpg is installed on your system.

On MacOS:
```bash
brew install gpg
```

Generate your key:
```bash
gpg --full-generate-key
```

Recommended settings:
- **key kind:** (1) RSA and RSA
- **key size:** 4096
- **key validity:** key does not expire
  (you can revoke keys, so unless you don't lose access to your key it is more convenient)
- **real name:** it is recommended using your real name
- **email address:** it is recommended to use the same email address here that you use to commit your work
- **comment:** it is recommended to use different keys for different use-cases / organizations.
  If you use the same email across organizations, you can distinguish your keys with the help of this field.
  eg.: "CODE SIGNING KEY" or "DATASQRL CODE SIGNING KEY"

You can create the new key by selecting "(O)kay"

To view your key, you issue this command:
```bash
gpg --list-secret-keys --keyid-format=long
```

The output should look like this:
```
sec   rsa4096/D2A162EAE1016F3G 2024-04-05 [SC]
      AFB8C2DEFEA93470D81C84E7D2A162EAE1016F3G
uid                 [ultimate] John Doe (CODE SIGNING KEY) <john.doe@example.com>
ssb   rsa4096/2F7B9EAC4D6F8150 2024-04-05 [E]
```

To use the above key to sign your commits cd into a repository and issue these commands:
```bash
git config user.signingkey D2A162EAE1016F3G
git config commit.gpgsign true
```

You also need to add the public key to your github profile for the signing to be verified. 

To do so, go to your github settings page, select the `SSH and GPG keys` tab. 

Press `New GPG Key`, then enter a name for the key and the outputs of the following command. 

```
gpg --armor --export D2A162EAE1016F3G
```

## License
By contributing to DataSQRL, you agree that your contributions will be licensed under the Apache 2.0 License.
