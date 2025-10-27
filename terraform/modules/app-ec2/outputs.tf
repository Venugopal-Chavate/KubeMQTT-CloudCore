output "instance_ids" {
  description = "List of EC2 instance IDs"
  value       = aws_instance.app[*].id
}

output "instance_public_ips" {
  description = "List of public IP addresses"
  value       = aws_eip.app[*].public_ip
}

output "security_group_id" {
  description = "Security group ID"
  value       = aws_security_group.app.id
}