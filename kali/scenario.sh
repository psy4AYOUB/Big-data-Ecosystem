#!/bin/bash
# Scenario de demonstration labsecu : declenche R3, R2, R1 puis R4.
# Tests autorises, limites au laboratoire, comptes et mots de passe fictifs.
WEB=${CIBLE_WEB:-http://srv-linux}
SSH=${CIBLE_SSH:-192.168.50.20}
AD=${CIBLE_AD:-192.168.50.10}
DOM=${DOMAINE:-labsecu.lan}
cd ~/labsecu || exit 1
read -rsp "Mot de passe des comptes de test : " PW; echo

etape() { echo; echo "=== $1 ==="; }
etape "A. Trafic normal (aucune alerte attendue)"
for i in 1 2 3 4 5; do curl -s -o /dev/null -w "%{http_code} " "$WEB/"; done; echo
etape "B. Scan Web -> R3"
gobuster dir -u "$WEB" -w pages_test.txt -t 10 -q --no-error
etape "C. Force brute SSH -> R2"
hydra -l testeur_demo -P mdp_fictifs.txt -t 2 -I "ssh://$SSH"
etape "E. Force brute AD -> R1"
netexec smb "$AD" -d "$DOM" -u charlie.lock -p mdp_fictifs.txt
etape "F. Succes apres echecs -> R4"
head -n 3 mdp_fictifs.txt > /tmp/mdp_3.txt
netexec smb "$AD" -d "$DOM" -u alice.test -p /tmp/mdp_3.txt
sleep 5
SORTIE=$(netexec smb "$AD" -d "$DOM" -u alice.test -p "$PW")
echo "${SORTIE//"$PW"/[mot de passe masque]}"   # ne jamais afficher le vrai mot de passe
echo; echo "Termine. Attendre 1 a 2 minutes puis regarder Kibana."
echo "Penser a deverrouiller sur SRV-AD : Unlock-ADAccount charlie.lock"
