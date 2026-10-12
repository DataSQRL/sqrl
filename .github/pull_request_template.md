## What does this change?

<!-- One or two sentences. Link the issue it fixes, e.g. "Closes #123". -->

## Area

- [ ] Planner / compiler (`sqrl-planner`)
- [ ] CLI (`sqrl-cli`)
- [ ] Server (`sqrl-server`)
- [ ] Deployment model (`sqrl-deployment-model`)
- [ ] Schema discovery (`sqrl-discovery`)
- [ ] Agent image and skills (`agent`)
- [ ] Tests (`sqrl-testing`)
- [ ] Build / CI
- [ ] Docs (`documentation`)

## How did you test it?

<!-- For example: added a snapshot test in sqrl-testing-integration, or ran `sqrl run` on an example project. -->

## Checklist

- [ ] The PR title starts with a type such as `feat:`, `fix:` or `docs:`
- [ ] `mvn clean install` passes locally and the code is formatted with `mvn -P dev initialize`
- [ ] New or changed behavior is covered by tests (`given_when_then` naming, AssertJ assertions)
- [ ] Snapshots are updated and the diffs are reviewed, if compiler output changed
- [ ] New dependency and plugin versions are defined as properties in the root `pom.xml`
- [ ] Commits are signed off and signed (`git commit -s -S`)
- [ ] Docs and agent skills are updated if user-facing behavior changed
