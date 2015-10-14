# -*- coding: utf-8 -*-

'''
Script que:

    - és per executar diàriament
    - itera les poblacions que troba en el fitxer 'poblaciones'
    - per cada població:

        - llegeix el link, que conté les prediccions meteorològiques per
            avui i demà.
            Les prediccions les van actualitzant diàriament.

        - Només ens interessa el tag sens_termica del dia ACTUAL


        - la sensació tèrmica ve en 4 tags, corresponent a les 6-12-18-24.
            Cal interpolar per tenir les 24 hores.


        - Recuperar la temperatura última (temperatura 24) del dia anterior,

            Si la tenim:
                per poder interpolar des de les 24 del dia anterior fins a
                les 6 del dia actual

            Si no la tenim: passem i interpolem amb 4 punts en comptes de 5.
            Llavors les primeres 6 hores seran totes iguals que la hora 6.

        - un cop interpolades, guardar al mongo les sensacions termiques
            de les 24 hores de la poblacio i dia corresponent (dia ACTUAL).

    - si hi ha hagut errors en la descàrrega de les prediccions, els logueja
        i envia un mail des del compte de correu de processos.

    PENDENT: Descarregar les prediccions del dia actual + el dia següent
            (DEMÀ), i en cada execució updatar el dia actual amb les noves
            prediccions. És necessari??? Potser no cal no?? Si aquestes
            temperatures són per a utilitzar a un any vista?

'''

import urllib2
import httplib
import os
import smtplib
import shutil
import click
import numpy
import json
from pymongo import MongoClient
from datetime import date
from datetime import datetime
from datetime import timedelta
import xml.etree.ElementTree as ET

from poblaciones import links_poblaciones
from credentials import mail_username, mail_pass, mail_host
from credentials import from_addr, to_addr

class XmlMalFormat(Exception):
    def __init__(self, missatge):
        self.message = missatge
        return


class RecullPrediccions(object):

    def __init__(self, dbname, colname, dburi):

        self.DBNAME = dbname
        self.COLNAME = colname
        self.DBURI = dburi

        self.poblaciones = links_poblaciones
        self.errors = {}

        self.mail_host = mail_host
        self.mail_username = mail_username
        self.mail_pass = mail_pass
        self.from_addr = from_addr
        self.to_addr = to_addr

    def mongoconnection(self):

        try:
            # Connectar i escollir la bbdd
            if self.DBURI:
                # Permet usr/pwd
                client = MongoClient(self.DBURI)

                self.mongodb = client[self.DBNAME]

                self.mongocol = self.mongodb[self.COLNAME]
                self.mongocol.ensure_index('dia')
                self.mongocol.ensure_index('poblacion')
            else:
                client = MongoClient()

                # Base de dades
                self.mongodb = client[self.DBNAME]

        except Exception as e:
            error = '''Error: No s'ha pogut connectar a la base de dades,
                        info: {0}\n'''.format(e.message)
            self.errors['base de dades'] = error

        return True

    def llegir_link(self, link):

        html = ''

        try:
            # llegir el contingut
            response = urllib2.urlopen(link)
            html = response.read(20000) # en principi ja hi cap tot
        except urllib2.HTTPError, e:
            self.errors[self.poblacion_actual] = 'HTTPError: {0}'.\
                                                        format(str(e.code))
        except urllib2.URLError, e:
            self.errors[self.poblacion_actual] = 'URLError: {0}'.\
                                                        format(str(e.reason))
        except httplib.HTTPException, e:
            self.errors[self.poblacion_actual] = 'HTTPException'
        except Exception:
            import traceback
            self.errors[self.poblacion_actual] = 'generic exception: {0}'.\
                                                format(traceback.format_exc())

        return html

    def crear_directori_avui(self):

        # posarem els fitxers en un directori amb nom del dia d'avui
        dir = 'predicciones/predicciones{0}'.\
                                    format(datetime.today().strftime('%Y%m%d'))
        self.directori = dir
        try:    os.makedirs(dir)
        except OSError as e:
            if e.errno == 17: # file exists, l'esborrem i el tornem a crear
                shutil.rmtree(dir)
                os.makedirs(dir)

        except Exception as e:
            self.errors['directori'] = str(e)

        return dir

    def interpolar(self, avui_hores, avui_dades, hora24ahir=None):
        '''Interpolem linealment les 4 o 5 dades que tinguem
        La hora 24 d'ahir és la hora 0 d'avui.

        S'ha d'indicar així perquè la llista de hores ha de ser ascendent
        perquè la interpolació funcioni

        Obtindrem una llista de 25 hores, que aniran des de la hora 0
        d'avui fins la hora 0 de demà (com si diguéssim hora 24 d'avui).

        D'aquestes 25 hores, només ens interessen les 24 últimes, des de la
        hora 1 d'avui fins la hora 24 d'avui.
        '''

        # insertem la hora 0 d'avui (hora 24 d'ahir) si la tenim
        if hora24ahir is not None:
            hora24ahir = float(hora24ahir)
            avui_hores.insert(0,0)
            avui_dades.insert(0,hora24ahir)

        st_interpolades = [numpy.interp(hora, avui_hores, avui_dades)
                                for hora in range(0,25)]

        # un cop ho tenim interpolat, si tenim 25 hores, treiem la 1a ja que
        # és la hora 24 d'ahir i ja la tenim
        if len(st_interpolades) == 25:
            st_interpolades.pop(0)

        return st_interpolades

    def guardar_registre(self, l25hores):

        registre = {
            'poblacion':    self.poblacion_actual,
                    'dia':          datetime.today(),
                    'h1':           l25hores[0],
                    'h2':           l25hores[1],
                    'h3':           l25hores[2],
                    'h4':           l25hores[3],
                    'h5':           l25hores[4],
                    'h6':           l25hores[5],
                    'h7':           l25hores[6],
                    'h8':           l25hores[7],
                    'h9':           l25hores[8],
                    'h10':          l25hores[9],
                    'h11':          l25hores[10],
                    'h12':          l25hores[11],
                    'h13':          l25hores[12],
                    'h14':          l25hores[13],
                    'h15':          l25hores[14],
                    'h16':          l25hores[15],
                    'h17':          l25hores[16],
                    'h18':          l25hores[17],
                    'h19':          l25hores[18],
                    'h20':          l25hores[19],
                    'h21':          l25hores[20],
                    'h22':          l25hores[21],
                    'h23':          l25hores[22],
                    'h24':          l25hores[23]
        }

        try:
            self.inserta(registre)

        except Exception as e:
            self.errors['{0}_guardar_a_mongo'.format(self.poblacion_actual)] = 1

        return

    def obtenir_hora_24_ahir(self):

        str_avui = str(date.today())
        avui_00 = datetime.strptime(str_avui, "%Y-%m-%d")
        ahir_00 = avui_00 - timedelta(days=1)

        # # buscar la hora 24 del dia anterior
        st24_ahir = self.mongocol.find(\
                    {
                        'poblacion' : self.poblacion_actual,
                        'dia':  {
                                '$gte': ahir_00,
                                '$lt' : avui_00
                                }
                    },
                    {'h24': 1,
                     '_id': 0
                    }
                )

        # retornem només el valor
        return st24_ahir[0]['h24']

    def get_dades_xml_correctes(self, contingut_xml):
        '''Comprovem si les dades del xml són tal com les esperem.
        Els tags de sensació tèrmica del dia d'avui de vegades són buits.'''

        try:
            # obtenim els dies que hi ha al xml
            element = ET.fromstring(contingut_xml)
            dias = element.find('prediccion').findall('dia')

            # dels dies que venen, busquem el dia d'avui. Pot ser que encara
            # hi sigui el dia d'ahir, depèn de quan actualitzen el fitxer.
            # No sempre està a la mateixa posició
            avui = str(date.today())
            for dia in dias:
                if dia.attrib['fecha'] == avui:
                    xmlelement_avui = dia
                    break

            # buscar del dia d'avui si els tags de sens_termica estàn correctes
            # transformem alhora que comprovem que tots els tags tenen dades
            xmlelement_sens_termica = xmlelement_avui.find('sens_termica')
            st_valores = [float(hora.text)
                      for hora in xmlelement_sens_termica.findall('dato')]
            st_horas = [int(hora.get('hora'))
                    for hora in xmlelement_sens_termica.findall('dato')]

            return st_horas, st_valores

        except Exception as e:
            self.errors['{0} xmlmalformat'.format(self.poblacion_actual)] \
                                                            = str(e)
            raise XmlMalFormat('xml amb dades incompletes o incorrectes')

    def processar_xml(self, contingut_xml):
        '''extreure les 4 dades de sensacio termica, interpolar i guardar
        el resultat al mongo'''

        # comprovem que les dades del xml són correctes
        # de vegades el tag de sensació tèrmica ve amb hores buides...
        avui_hores, avui_dades = self.get_dades_xml_correctes(contingut_xml)

        try:
            st24_ahir = self.obtenir_hora_24_ahir()
        except Exception as e:
            self.errors['{0}_error_obtenint_hora24'.
                format(self.poblacion_actual)] = str(e)
            st24_ahir = None

        # les hores han de ser ascendents per què la interpolació funcioni
        if numpy.all(numpy.diff(avui_hores) > 0): pass
        else: raise XmlMalFormat('xml amb hores no ascendents')

        # interpolar les 4 dades (o 5 si tenim la h 24 del dia anterior)
        l24h = self.interpolar(avui_hores, avui_dades, hora24ahir=st24_ahir)

        # guardar el registre al mongo
        self.guardar_registre(l24h)

        return True

    def guardar_fitxer_xml(self, contingut_xml):

        try:
            nom_fitxer = '{0}/{1}_{2}.xml'.\
                format(self.directori,
                       self.poblacion_actual,
                       datetime.now().strftime('%Y%m%d%H%M%S'))

            # escriure les dades al fitxer amb la data d'avui
            with open(nom_fitxer, 'w') as f:
                f.write(contingut_xml)
            return True

        except Exception as e:
            self.errors['{0}_guardar_fitxer'] = str(e)
            return False

    def enviar_error_mail(self):

        server = smtplib.SMTP(self.mail_host)
        server.login(self.mail_username, self.mail_pass)
        fromaddr = self.from_addr
        toaddrs = self.to_addr
        subject = 'Errors en recollidor prediccions temperatures'
        msg = json.dumps(self.errors, indent=2)
        msg = 'From: {0}\nSubject: {1}\n\n{2}'.format(fromaddr,subject,msg)
        server.sendmail(fromaddr, toaddrs, msg)
        server.quit()

        return True

    def loguejar_errors(self):

        with open('/var/log/temperatures/log.txt', 'a') as f:
            f.write('Execució {0}'.format(datetime.today()))
            f.write(json.dumps(self.errors, indent=2))
            f.write('\n')
        return True

    def run(self):

        self.crear_directori_avui()
        self.mongoconnection()

        # si el directori s'ha creat correctament
        if not 'directori' in self.errors:

            for poblacion, link in links_poblaciones.iteritems():

                self.poblacion_actual = poblacion

                xmlstring = self.llegir_link(link)

                if poblacion in self.errors:
                    # si el link ha donat error, seguim amb el següent link
                    continue

                try:
                    self.processar_xml(xmlstring)

                except Exception as e:
                    # si tenim algun error guardem el fitxer sencer
                    self.guardar_fitxer_xml(xmlstring)

                    # indiquem que hi ha hagut un error en el processament
                    missatge = '{0}, {1}'.format(
                        str(e),
                        'xml guardat a /home/abenergia_admin/predicciones...'
                    )
                    self.errors['{0}_proces_xml'.format(poblacion)] = missatge

        if self.errors:
            self.loguejar_errors()
            self.enviar_error_mail()

        return True

    def inserta(self, registre):
        '''Inserta el registre afegint-li la clau 'id'.
        'id' correspon al comptador de la coleccio. Si no existeix comptador
        per a la coleccio el crea.'''

        counter = self.mongodb['counters'].find_and_modify(
            {'_id': self.COLNAME},
            {'$inc': {'counter': 1}}
        )
        if counter is None:
            counter = {'_id' : self.COLNAME,
                         'counter' : 1}
            self.mongodb['counters'].insert(counter)

        registre.update({'id': counter['counter']})
        oid = self.mongocol.insert(registre)

        return oid

@click.command()
@click.option('--dbname', default='abenergia', help='Nom de la bd.')
@click.option('--colname', default='temperatures', help='Nom de la colecció.')
@click.option('--dburi', default='mongodb://192.168.0.24/abenergia')

def main(dbname, colname, dburi):

    recollidor = RecullPrediccions(dbname, colname, dburi)
    recollidor.run()

if __name__ == '__main__':
    main()
