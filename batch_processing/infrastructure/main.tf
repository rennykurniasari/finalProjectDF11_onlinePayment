resource "google_compute_firewall" "airflow_firewall" {
  name    = "airflow-1"
  network = var.network

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
  machine_type = var.vm_type
  zone         = "asia-southeast2-a"

  tags = ["airflow-1"]


  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    network = var.network
    access_config {
      nat_ip = google_compute_address.batch_processing_address.address
    }
  }

  metadata = {
    ssh-keys = "${var.user_gcp}:${file(var.public_key_vm)}"
  }

  connection {
      type     = "ssh"
      user     = var.user_gcp
      host     = google_compute_address.batch_processing_address.address
      private_key = "${file(var.private_key_vm)}"
    }

  provisioner "file" {
    source = "../docker-compose.yml"
    destination = "/home/${var.user_gcp}/docker-compose.yml"
  }

  provisioner "file" {
    source = "../dags/online_payment_log_Oct2023.py"
    destination = "/home/${var.user_gcp}/online_payment_log_Oct2023.py"
  }

  provisioner "file" {
    source = "../.env"
    destination = "/home/${var.user_gcp}/.env"
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

resource "google_bigquery_dataset" "raw_table" {
  dataset_id = "online_payment"
  project    = var.project
  location   = "US"
}