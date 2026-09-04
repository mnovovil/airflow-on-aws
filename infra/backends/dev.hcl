# Backend config for dev:  terraform init -backend-config=backends/dev.hcl
#
# Same bucket as prod's state, different key. Sharing the bucket is fine — the lock
# file is per key, so a dev apply and a prod apply do not block each other — and it
# keeps the bootstrap stack at exactly one bucket to protect.
key = "ice/dev/terraform.tfstate"
