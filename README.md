# sqml

## Setup

1. Git clone repository
1. Open maven project in IntelliJ (if module structure is not correctly recognized, close IntelliJ, delete `.idea` folder, and reopen IntelliJ)
1. Initialize sqml-examples submodule (`git submodule init` and `git submodule update`)
1. Generate ANTLR classes: In the `sqml-parser` module run the maven goal `compile`
1. Run full maven build
