terraform {
    required_providers {
        google = {
            source = "hashicorp/google"
            version = "4.51.0"
        }
    }
}

provider "google" {
    project = "test-terraform-405913"
    region = "asia-southeast2"
    credentials = file("../keys/test-terraform-sa.json")
}

resource "google_compute_firewall" "airflow_firewall" {
  name    = "airflow-1"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }

  source_ranges = ["0.0.0.0/0"]  # Bisa disesuaikan dengan alamat IP yang diizinkan
  target_tags = ["airflow-1"]
}

resource "google_compute_address" "batch_processing_address" {
  name   = "batch-processing"
  region = "asia-southeast2"  # Ganti dengan region yang sesuai
}

resource "google_compute_instance" "batch_processing_instance" {
  name         = "batch-processing"
  machine_type = "e2-standard-4"
  zone         = "asia-southeast2-a"

  tags = ["airflow-1"]


  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    network = "default"
    access_config {
      nat_ip = google_compute_address.batch_processing_address.address
    }
  }

  metadata = {
    ssh-keys = "riswanda_work:${file("../keys/terraform-key.pub")}"
  }

  connection {
      type     = "ssh"
      user     = "riswanda_work"
      host     = google_compute_address.batch_processing_address.address
      private_key = "${file("../keys/terraform-key")}"
    }

  provisioner "file" {
    source = "../docker-compose.yml"
    destination = "/home/riswanda_work/docker-compose.yml"
  }

  provisioner "file" {
    source = "../dags/online_payment_log_Oct2023.py"
    destination = "/home/riswanda_work/online_payment_log_Oct2023.py"
  }

  provisioner "file" {
    source = "../.env"
    destination = "/home/riswanda_work/.env"
  }

  provisioner "remote-exec" {
    inline = [
      "sudo DEBIAN_FRONTEND=noninteractive apt-get install -y python3-pip",
      "curl -fsSL https://get.docker.com -o get-docker.sh",
      "sudo sh get-docker.sh",
      "pip3 install dbt-bigquery",
      "mkdir dags",
      "mv online_payment_log_Oct2023.py dags"
    ]
  }

}