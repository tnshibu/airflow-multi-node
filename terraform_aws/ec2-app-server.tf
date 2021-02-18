
# Create Ubuntu Server and install/enable required software
resource "aws_instance" "my_ec2_app_server" {
  ami                    = "ami-0a91cd140a1fc148a"
  instance_type          = "t3.large"
  #availability_zone     = "us-east-2a"
  key_name               = aws_key_pair.webserver_key.key_name
  subnet_id              = aws_subnet.my_subnet.id
  vpc_security_group_ids = [aws_security_group.my_sec_grp.id]
  associate_public_ip_address = true
  
  user_data = <<-EOF
		#! /bin/bash
        sudo apt-get update
		#sudo apt-get install -y python3-pip 
        #sudo apt-get install -y postgresql postgresql-contrib redis 
        #sudo pip3 install --upgrade pip
		#pip3 install apache-airflow[postgres,celery,redis]
        #echo "export PATH=$PATH:$HOME/.local/bin" >> ~/.bashrc
        #source ~/.bashrc
        
        sudo apt-get remove docker docker-engine docker.io containerd runc
        sudo apt-get update
        sudo apt-get install apt-transport-https ca-certificates curl gnupg-agent software-properties-common -y
        curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
        sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
        sudo apt-get update
        sudo apt-get install docker-ce docker-ce-cli containerd.io -y
        sudo groupadd docker
        sudo gpasswd -a $USER docker

        sudo apt-get update
        sudo apt-get install jq -y
        VERSION=$(curl --silent https://api.github.com/repos/docker/compose/releases/latest | jq .name -r)
        sudo curl -L https://github.com/docker/compose/releases/download/1.28.2/docker-compose-$(uname -s)-$(uname -m) -o docker-compose
        sudo cp docker-compose /usr/local/bin/docker-compose
        sudo chmod 755 /usr/local/bin/docker-compose
        sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
        docker-compose --version

	EOF

  tags = {
    Name = "my_ec2_app_server"
    project = "p1"
  }
}

#---------------------------------------------------------------------------
#Print 
output "my_ec2_app_server_ip_address" {
  value = aws_instance.my_ec2_app_server.public_ip
}
#---------------------------------------------------------------------------
