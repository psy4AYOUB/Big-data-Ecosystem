# Machine d'attaque du laboratoire labsecu (remplace la VM KALI pour les tests)
# Outils : nmap, hydra, gobuster, netexec, smbclient + listes de test FICTIVES
FROM kalilinux/kali-rolling
LABEL org.opencontainers.image.source="https://github.com/psy4AYOUB/Big-data-Ecosystem"
LABEL org.opencontainers.image.description="labsecu - conteneur d'attaque (tests autorises, comptes fictifs)"

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      nmap hydra gobuster netexec smbclient curl ca-certificates iputils-ping dnsutils \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN useradd -m -s /bin/bash attaquant
COPY --chown=attaquant:attaquant listes/ /home/attaquant/labsecu/
COPY scenario.sh /usr/local/bin/scenario-labsecu
RUN chmod +x /usr/local/bin/scenario-labsecu
USER attaquant
WORKDIR /home/attaquant/labsecu
CMD ["/bin/bash"]
