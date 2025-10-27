output "vpc_id" {
  description = "VPC ID"
  value       = module.network.vpc_id
}

output "app_instance_ips" {
  description = "Application instance public IPs"
  value       = module.app_ec2.instance_public_ips
}

output "app_instance_ids" {
  description = "Application instance IDs"
  value       = module.app_ec2.instance_ids
}