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

                if self.DBNAME: # nom de la bd
                    self.mongodb = client[self.DBNAME]

                    if self.COLNAME: # nom de la coleccio
                        self.mongodb = self.mongodb[self.COLNAME]

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
        try:    os.makedirs(dir)
        except OSError as e:
            if e.errno == 17: # file exists, l'esborrem i el tornem a crear
                shutil.rmtree(dir)
                os.makedirs(dir)

        except Exception as e:
            self.errors['directori'] = str(e)

        return dir

    def interpolar_hores(self, dia, hora24ahir=None):
        '''Interpolem linealment les 4 o 5 dades que tinguem
        La hora 24 d'ahir és la hora 0 d'avui.

        S'ha d'indicar així perquè la llista de hores ha de ser ascendent
        perquè la interpolació funcioni

        Obtindrem una llista de 25 hores, que aniran des de la hora 0
        d'avui fins la hora 0 de demà (com si diguéssim hora 24 d'avui).

        D'aquestes 25 hores, només ens interessen les 24 últimes, des de la
        hora 1 d'avui fins la hora 24 d'avui.
        '''
        sens_termica_elem = dia.find('sens_termica')
        st_valores = [int(hora.text)
                      for hora in sens_termica_elem.findall('dato')]
        st_horas = [int(hora.get('hora'))
                    for hora in sens_termica_elem.findall('dato')]

        # reemplacem la hora 0 d'avui si la tenim
        if hora24ahir is not None:
            hora0avui = hora24ahir
            st_horas.insert(0,0)
            st_valores.insert(0,hora0avui)

        # la llista d'hores ha de ser ascendent perquè la interp. funcioni
        if numpy.all(numpy.diff(st_horas) > 0):
            st25horas = [numpy.interp(hora, st_horas, st_valores)
                            for hora in range(0,25)]

            # posem les tempratures amb 1 sol decimal
            st25horas_format = ["{:10.1f}".format(t) for t in st25horas]

        else:
            raise XmlMalFormat('xml amb hores no ascendents\n')

        return st25horas_format

    def guardar_registre(self, l25hores):
        # no guardem la posició 0 ja que és la hora 24 d'ahir

        registre = {
            'poblacion':    self.poblacion_actual,
                    'dia':          datetime.today(),
                    'h1':           l25hores[1],
                    'h2':           l25hores[2],
                    'h3':           l25hores[3],
                    'h4':           l25hores[4],
                    'h5':           l25hores[5],
                    'h6':           l25hores[6],
                    'h7':           l25hores[7],
                    'h8':           l25hores[8],
                    'h9':           l25hores[9],
                    'h10':          l25hores[10],
                    'h11':          l25hores[11],
                    'h12':          l25hores[12],
                    'h13':          l25hores[13],
                    'h14':          l25hores[14],
                    'h15':          l25hores[15],
                    'h16':          l25hores[16],
                    'h17':          l25hores[17],
                    'h18':          l25hores[18],
                    'h19':          l25hores[19],
                    'h20':          l25hores[20],
                    'h21':          l25hores[21],
                    'h22':          l25hores[22],
                    'h23':          l25hores[23],
                    'h24':          l25hores[24]
        }

        try:
            self.mongodb.insert(registre)
        except Exception as e:
            self.errors['{0}_guardar_a_mongo'.format(self.poblacion_actual)] = 1

        return

    def obtenir_hora_24_ahir(self, dia_xml):

        avui_00 = datetime.strptime(dia_xml, "%Y-%m-%d")
        ahir_00 = avui_00 - timedelta(days=1)

        # # buscar la hora 24 del dia anterior
        st24_ahir = self.mongodb.find(\
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

    def processar_xml(self, contingut_xml):
        '''extreure les 4 dades de sensacio termica, interpolar i guardar
        el resultat al mongo'''

        element = ET.fromstring(contingut_xml)

        dias = element.find('prediccion').findall('dia')
        avui = str(date.today())

        dades_avui = dias[0]
        dia_xml = dades_avui.get('fecha')

        if dia_xml == avui:

            try:
                st24_ahir = self.obtenir_hora_24_ahir(dia_xml)
            except:

                st24_ahir = None
                self.errors['{0}_hora24'.format(self.poblacion_actual)] \
                                                                = 'no existeix'

            # interpolar les 4 dades (o 5 si tenim la h 24 del dia anterior)
            l24_hores = self.interpolar_hores(dades_avui, hora24ahir=st24_ahir)

            # guardar el registre al mongo
            self.guardar_registre(l24_hores)

        else:
            # excepcio xml malformat
            raise XmlMalFormat('Dates incorrectes o en ordre incorrecte\n')

        return True

    def guardar_fitxer_xml(self, contingut_xml, poblacion):

        try:
            nom_fitxer = '{0}/{1}_{2}.xml'.\
                format(self.directori,
                       poblacion,
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
                    # si tenim algun error parsejant el contingut xml,
                    # guardem el fitxer sencer
                    self.guardar_fitxer_xml(self, xmlstring, poblacion)

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

@click.command()
@click.option('--dbname', default='temperatures', help='Nom de la bd.')
@click.option('--colname', default='temperatures', help='Nom de la colecció.')
@click.option('--dburi', default='mongodb://localhost/temperatures')

def main(dbname, colname, dburi):

    recollidor = RecullPrediccions(dbname, colname, dburi)
    recollidor.run()

if __name__ == '__main__':
    main()
