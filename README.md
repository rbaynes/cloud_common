# cloud-common
Common classes shared by our GCloud hosted services.

This repo is intended to be used as a git submodule in our hosted services.
When you create a new service, this is how you set up a submodule:

```bash
cd my-new-service-that-is-already-a-git-repo
git submodule add -b master https://github.com/OpenAgricultureFoundation/cloud-common
git submodule init
```

When you want to pull the latest master branch version of this project:
```bash
git submodule update --remote
```

