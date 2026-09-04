# Backend config for prod:  terraform init -backend-config=backends/prod.hcl
#
# This key is the one the stack has always used. It must not change — moving it
# would orphan the live state and the next apply would try to build a second copy
# of everything on top of the first.
key = "ice/terraform.tfstate"
