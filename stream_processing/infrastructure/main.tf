resource "google_storage_bucket" "online_payment" {
  name          = "online-payment-oct"
  location      = "asia-southeast2"
  force_destroy = true

  versioning {
    enabled = true
  }
}

resource "google_storage_bucket_object" "csv_dataset" {

  name   = var.name_dataset
  content_type = "text/csv"
  source = "${path.module}/../../Fraud Transaction Dataset/${var.name_dataset}"
  bucket = var.bucket_gcs
}

resource "google_storage_bucket_object" "folder_email_confirmation" {

  name   = var.folder_gcs_email
  content = " "
  bucket = var.bucket_gcs
}

resource "google_storage_bucket_object" "folder_fraud_confirmation" {

  name   = var.folder_gcs_fraud
  content = " "
  bucket = var.bucket_gcs
}

resource "google_storage_bucket_object" "folder_transaction" {

  name   = var.folder_gcs_transaction
  content = " "
  bucket = var.bucket_gcs
}

resource "google_compute_firewall" "kafka_firewall" {
  name    = "kafka"
  network = var.network

  allow {
    protocol = "tcp"
    ports    = ["9092","9999","9093","9998","9094","9997","8081","9080"]
  }

  source_ranges = ["0.0.0.0/0"]  # Bisa disesuaikan dengan alamat IP yang diizinkan
  target_tags = ["kafka"]
}

resource "google_compute_address" "kafka_address" {
  name   = "kafka"
  region = "asia-southeast2"  # Ganti dengan region yang sesuai
}

resource "google_compute_instance" "kafka_instance" {
  name         = "kafka"
  machine_type = var.vm_type
  zone         = "asia-southeast2-a"

  tags = ["kafka"]


  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    network = var.network
    access_config {
      nat_ip = google_compute_address.kafka_address.address
    }
  }

  metadata = {
    ssh-keys = "${var.user_gcp}:${file(var.public_key_vm)}"
  }

  connection {
      type     = "ssh"
      user     = var.user_gcp
      host     = google_compute_address.kafka_address.address
      private_key = "${file(var.private_key_vm)}"
    }

  provisioner "file" {
    source = "../docker-compose.yml"
    destination = "/home/${var.user_gcp}/docker-compose.yml"
  }

  provisioner "remote-exec" {
    inline = [
      "curl -fsSL https://get.docker.com -o get-docker.sh",
      "sudo sh get-docker.sh",
      "mkdir kafka",
      "mkdir kafka/data",
      "mkdir kafka/data/kafka-1",
      "mkdir kafka/data/kafka-2",
      "mkdir kafka/data/kafka-3",
      "mkdir zookeeper",
      "mkdir zookeeper/data",
      "mkdir zookeeper/data/datalog"
    ]
  }
}

resource "google_compute_address" "producer_address" {
  name   = "producer"
  region = "asia-southeast2"  # Ganti dengan region yang sesuai
}

resource "google_compute_instance" "producer_instance" {
  name         = "producer"
  machine_type = var.vm_type
  zone         = "asia-southeast2-a"

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    network = var.network
    access_config {
      nat_ip = google_compute_address.producer_address.address
    }
  }

  metadata = {
    ssh-keys = "${var.user_gcp}:${file(var.public_key_vm)}"
  }

  connection {
      type     = "ssh"
      user     = var.user_gcp
      host     = google_compute_address.producer_address.address
      private_key = "${file(var.private_key_vm)}"
    }

  provisioner "file" {
    source = "../producer/produce_transaction.py"
    destination = "/home/${var.user_gcp}/produce_transaction.py"
  }

  provisioner "file" {
    source = "../requirements.txt"
    destination = "/home/${var.user_gcp}/requirements.txt"
  }

  provisioner "file" {
    source = "../schema/TransactionSchema.avsc"
    destination = "/home/${var.user_gcp}/TransactionSchema.avsc"
  }

  provisioner "remote-exec" {
    inline = [
      "mkdir schema",
      "mv TransactionSchema.avsc schema",
      "sudo DEBIAN_FRONTEND=noninteractive apt-get install -y python3-pip",
      "pip3 install -r requirements.txt"
    ]
  }
}

resource "google_compute_address" "consumer_not_fraud_address" {
  name   = "consumer-not-fraud"
  region = "asia-southeast2"  # Ganti dengan region yang sesuai
}

resource "google_compute_instance" "consumer_not_fraud_instance" {
  name         = "consumer-not-fraud"
  machine_type = var.vm_type
  zone         = "asia-southeast2-a"

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    network = var.network
    access_config {
      nat_ip = google_compute_address.consumer_not_fraud_address.address
    }
  }

  metadata = {
    ssh-keys = "${var.user_gcp}:${file(var.public_key_vm)}"
  }

  connection {
      type     = "ssh"
      user     = var.user_gcp
      host     = google_compute_address.consumer_not_fraud_address.address
      private_key = "${file(var.private_key_vm)}"
    }

  provisioner "file" {
    source = "../consumer/consumer_non_fraud_transaction.py"
    destination = "/home/${var.user_gcp}/consumer_non_fraud_transaction.py"
  }

  provisioner "file" {
    source = "../requirements.txt"
    destination = "/home/${var.user_gcp}/requirements.txt"
  }

  provisioner "file" {
    source = "../schema/TransactionSchema.avsc"
    destination = "/home/${var.user_gcp}/TransactionSchema.avsc"
  }

  provisioner "remote-exec" {
    inline = [
      "mkdir schema",
      "mv TransactionSchema.avsc schema",
      "sudo DEBIAN_FRONTEND=noninteractive apt-get install -y python3-pip",
      "pip3 install -r requirements.txt"
    ]
  }
}

resource "google_compute_address" "consumer_fraud_address" {
  name   = "consumer-fraud"
  region = "asia-southeast2"  # Ganti dengan region yang sesuai
}

resource "google_compute_instance" "consumer_fraud_instance" {
  name         = "consumer-fraud"
  machine_type = var.vm_type
  zone         = "asia-southeast2-a"

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    network = var.network
    access_config {
      nat_ip = google_compute_address.consumer_fraud_address.address
    }
  }

  metadata = {
    ssh-keys = "${var.user_gcp}:${file(var.public_key_vm)}"
  }

  connection {
      type     = "ssh"
      user     = var.user_gcp
      host     = google_compute_address.consumer_fraud_address.address
      private_key = "${file(var.private_key_vm)}"
    }

  provisioner "file" {
    source = "../consumer/consumer_fraud_transaction.py"
    destination = "/home/${var.user_gcp}/consumer_fraud_transaction.py"
  }

  provisioner "file" {
    source = "../requirements.txt"
    destination = "/home/${var.user_gcp}/requirements.txt"
  }

  provisioner "file" {
    source = "../schema/TransactionSchema.avsc"
    destination = "/home/${var.user_gcp}/TransactionSchema.avsc"
  }

  provisioner "remote-exec" {
    inline = [
      "mkdir schema",
      "mv TransactionSchema.avsc schema",
      "sudo DEBIAN_FRONTEND=noninteractive apt-get install -y python3-pip",
      "pip3 install -r requirements.txt"
    ]
  }
}