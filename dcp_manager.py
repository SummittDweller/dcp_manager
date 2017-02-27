#!/usr/bin/env python
# coding: utf8

import os
import fnmatch
import argparse
import logging
import datetime
import netrc
import xmltodict
import re
import subprocess
from lxml import etree
from email.mime.text import MIMEText
from smtplib import SMTP  # use this for standard SMTP protocol (TLS encryption)


# -------------------------------
class dcp_manager():
  """
  This script is used by the Wieting Theatre (Toledo, IA) to manage the ingest and storage
  of DCP cinema packages, mostly trailers.

  A logfile and a mail can be specified to be informed about operations.
  """

  # ------------------------------
  # Constants
  SOURCE = "/mnt/trailers"
  DESTINATION = "/mnt/trailers"
  MAIL = "toledowieting@gmail.com"
  LOGFILE = "/tmp/dcp_manger.log"
  DEBUG = False
  SCOPE = False
  NOT_TLR = False

  # -------------------------------
  # Perform the specified OPERATION !!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  #
  def main(self):

    self.pCount = 0   # number of packages
    self.aCount = 0   # number of assets found (the files exist)
    self.aTotal = 0   # total number of assets identifed
    self.pSkip = 0    # number of packages skipped due to missing assets or ignored format
    self.numCopy = 0  # number of rsync copy operations performed
    self.numDel = 0   # number of assests deleted

    self.parse_args()
    self.return_value = 0
    if self.logfile:
      self.logger.info("Saving " + self.op + " operation logfile to " + self.logfile + ".")
    else:
      self.logger.info("Starting " + self.op + " operation without a logfile.")

    # The operation switch...
    if self.op.lower() == 'catalog':
      out = self.catalog()
    elif self.op.lower() == 'copy':
      out = self.copy()
    elif self.op.lower() == 'delete':
      out = self.delete()
    else:
      msg = "The specifed '" + self.op + "' is NOT supported.  Exiting..."
      self.logger.info(msg)
      if self.mail:
        self.send_mail(self.mail, self.logfile, 400, msg)
      exit(1)

    self.logger.info("Operation " + self.op + " is complete with a return code " + str(self.return_value) + ".")
    if self.mail:
      self.send_mail(self.mail, self.logfile, self.return_value, out)

  # -------------------------------
  # CATALOG - Prepare an ASSETMAP file reflecting contents of the self.source directory.
  #
  # Logic to append data to the existing XML is from
  # http://stackoverflow.com/questions/3648689/python-lxml-append-a-existing-xml-with-new-data
  # and from the lxml tutorial at http://lxml.de/tutorial.html.
  #
  def catalog(self):
    # Check for an ASSETMAP file in self.dest
    if os.path.exists(self.destAssetXML):
      self.logger.warning("Be advised, the target ASSETMAP file already exists.  It will be replaced.")
      os.remove(self.destAssetXML)
    else:
      self.logger.info("No target " + self.destAssetXML + " found so a new one will be created.")

    self.initAssetMap()  # Initialize a new empty ASSETMAP

    # Find all PKL files in the source directory
    pkls = []
    for root, dirnames, filenames in os.walk(self.source):
      for filename in fnmatch.filter(filenames, '[Pp][Kk][Ll]*.[Xx][Mm][Ll]'):
        path = root + "/" + filename
        pkls.append(path)

    # Open self.destAssetXML and append the AssetList tag
    tree = etree.parse(self.destAssetXML)
    root = tree.getroot()
    assetList = etree.SubElement(root, 'AssetList')

    # Loop on the found PKL files
    for pkl in pkls:
      self.pkl = pkl
      self.assets = []
      self.package = self.fetchPKLAssets()
      self.pCount += 1

      # Determine if the package should be skipped...
      if self.skipPackage():
        continue

      # Found a package.  Determine if all of its assets are present.
      if self.packageAssetsMissing():
        continue

      # Loop again and catalog the complete package
      for a in self.assets:
        filename = self.remove_prefix(a['file'], self.source + "/")
        asset = etree.SubElement(assetList, "Asset")
        etree.SubElement(asset, "Id").text = a['id']
        if 'PKL_' in filename:
          etree.SubElement(asset, "PackingList").text = 'true'
        chunkList = etree.SubElement(asset, "ChunkList")
        chunk = etree.SubElement(chunkList, "Chunk")
        etree.SubElement(chunk, "Path").text = filename
        etree.SubElement(chunk, "VolumeIndex").text = '1'
        etree.SubElement(chunk, "Length").text = str(a['size'])

        # Update the ASSETMAP file after each asset is added
        xml = etree.ElementTree(root)
        xml.write(self.destAssetXML, pretty_print=True, xml_declaration=True)

    parser = etree.XMLParser(remove_blank_text=True)
    tree = etree.parse(self.destAssetXML, parser)
    print etree.tostring(tree, pretty_print=True, xml_declaration=True)
    print ""

    # Pretty print the output per http://stackoverflow.com/questions/33567245/run-console-command-with-python
    with open(self.destAssetMap, 'w') as outfile:
      subprocess.call(["xmllint", "--format", self.destAssetXML], stdout=outfile)

    self.logger.info(
      "CATALOG operation is complete with " + str(self.pSkip) + " of " + str(self.pCount) + " packages skipped, and " +
      str(self.aCount) + " of " + str(self.aTotal) + " possible assets found.")


  #--------------------------
  # skipPackage
  def skipPackage(self):
    # If self.scope is False, check the package name and skip it if '_S_' is part of the name
    if not self.scope:
      if '_S_' in self.package:
        self.pSkip += 1
        self.logger.info("Package '" + self.package + "' is in SCOPE and has been omitted.")
        return True
    # If self.not_tlr is False, check the package name and skip it if '_TLR' is not part of the name
    if not self.not_tlr:
      if '_TLR' not in self.package:
        self.pSkip += 1
        self.logger.info("Package '" + self.package + "' is not a TRAILER and has been omitted.")
        return True
    return False


  # -------------------------------
  # COPY - Execute rsync for asset files found in the self.source directory.
  #
  def copy(self):

    # Find all PKL files in the source directory
    pkls = []
    for root, dirnames, filenames in os.walk(self.source):
      for filename in fnmatch.filter(filenames, '[Pp][Kk][Ll]*.[Xx][Mm][Ll]'):
        path = root + "/" + filename
        pkls.append(path)

    self.destAssetMap = "/tmp/ASSETMAP"

    # Loop on the found PKL files
    packages = {}  # create an empty dictionary
    for pkl in pkls:
      self.pkl = pkl
      self.assets = []
      self.package = self.fetchPKLAssets()
      self.pCount += 1
      self.missing = 0

      # Determine if package should be skipped...
      if self.skipPackage():
        continue

      # Found a package.  Determine if all of its assets are present.
      if self.packageAssetsMissing( ):
        continue

      # Save the package (name, path) in the packages dict.
      packages[self.package] = pkl

    # Now loop on all the packages, find similarly named packages
    for self.package in packages:
      key = self.package.split('_', 1)[0]

      self.similar = [(k, v) for (k, v) in packages.iteritems() if key in k]
      num_similar = len(self.similar)
      self.logger.info("There are " + str(num_similar) + " packages with a key similar to " + key)

      # Now find the best 2D, and maybe 3D, pacakges to keep
      self.keep = {}
      self.keepBestPackages('2D')
      self.keepBestPackages('3D')

      # Ok, build a set of rsync commands to make the copies
      self.numCopy += self.buildCopyScript( )

    self.logger.info(
      "COPY operation is complete with " + str(self.pSkip) + " of " + str(self.pCount) + " packages skipped, and " +
      str(self.aCount) + " of " + str(self.aTotal) + " possible assets found. " + str(self.numCopy) + " rsync operations were peformed.")


  # -------------------------------
  # DELETE - Remove asset files of --packages pattern from self.dest.
  #
  def delete(self):
    nMatch = 0
    # Find all PKL files in the destination directory
    pkls = []
    for root, dirnames, filenames in os.walk(self.dest):
      for filename in fnmatch.filter(filenames, '[Pp][Kk][Ll]*.[Xx][Mm][Ll]'):
        path = root + "/" + filename
        pkls.append(path)

    # Loop on the found PKL files
    for pkl in pkls:
      self.pkl = pkl
      self.assets = []
      self.package = self.fetchPKLAssets()
      self.pCount += 1

      pattern = "r'" + self.pkgPattern + "'"
      if re.search(pattern, self.package):
        nMatch += 1
        # Loop again on the assets and remove them
        for a in self.assets:
          filename = self.remove_prefix(a['file'], self.dest + "/")
          os.remove(filename)
          self.numDel += 1

    self.logger.info(
      "DELETE operation is complete with the '" + self.pkgPattern + "' matching " + str(nMatch) + " packages.  " + str(self.numDel) + " assets were deleted.")


  #--------------------------------
  # packageAssetsMissing
  def packageAssetsMissing(self):
    missing = 0
    for a in self.assets:
      self.aTotal += 1
      filename = a['file']
      target = self.source + "/" + filename  # @TODO...assumes the asset is in self.source directory
      # First, determine if the file exists...
      if filename == 'None':
        missing += 1
      elif not os.path.isfile(target):
        missing += 1
        self.logger.warning("File " + target + ", part of " + self.package + ", was NOT found.")
      else:
        self.aCount += 1

    # If assets are missing, skip the package.
    if missing > 0:
      self.logger.warning("Package '" + self.package + "' is missing " + str(missing) + " element(s) and has been omitted from the catalog.")
      self.pSkip += 1

    return missing


  #--------------------------------
  # buildCopyScript
  def buildCopyScript(self):
    count = 0
    for type in ['2D', '3D']:
      if type in self.keep:
        (k, v) = self.keep[type]
        self.pkl = v
        self.assets = []
        package = self.fetchPKLAssets()
        for a in self.assets:
          filename = a['file']
          s = self.source + "/" + filename  # @TODO...assumes the asset is in self.source directory
          if os.path.isfile(s):
            subprocess.call(["rsync", "-aruvi", s, self.dest])
            self.logger.info("Subprocess call to rsync for '" + s +"'.")
            count += 1
    return count


  #----------------------------------
  # findLatestVersion
  def findLatestVersion(self, type, key, value):
    try:
      version = re.search('_TLR((-)?[A-Z0-9])', key).group(1)
      version = ord(version.strip('-'))
    except AttributeError:
      version = 0
    if version > self.tlrVersion:
      self.tlrVersion = version
      self.keep[type] = (key, value)


  # ---------------------------------
  # keepBestPackages
  def keepBestPackages(self, type='2D'):
    self.tlrVersion = -1
    for k, v in self.similar:
      if type == '2D' and '3D' not in k:
        self.findLatestVersion(type, k, v)
      elif type == '3D' and '3D' in k:
        self.findLatestVersion(type, k, v)
    if type in self.keep:
      (k, v) = self.keep[type]
      self.logger.info("COPY will keep " + type + " package " + k + ".")
    else:
      self.logger.warning("COPY found NO " + type + " package to keep!")
    return


  # --------------------------------
  # fetchPKLAssets
  #
  # Fetch the list of assets from a PKL file.  Logic is lifted from
  # http://docs.python-guide.org/en/latest/scenarios/xml/.
  #
  # self.pkl - The PKL filename.
  # self.packageName - Returned string containing the PKL AnnotationText as the pacakge name.
  # self.assets - Returned array of assets found. Each asset is another array with an index equal to its 'id',
  # a 'file' component and optional 'annotation'. Note that the PKL file itself MUST be prepended to the list of assets!
  #
  # Returns the AnnotationText value from the PKL or 0 if there was an error.
  #
  def fetchPKLAssets(self):
    with open(self.pkl) as fd:
      doc = xmltodict.parse(fd.read())

    # Save the AnnotationText as self.packageName and prepend the PKL itself to the list of assets.
    self.packageName = doc['PackingList']['AnnotationText']
    asset = {}
    asset['id'] = doc['PackingList']['Id']
    asset['file'] = self.remove_prefix(self.pkl, self.source + '/')
    asset['size'] = os.path.getsize(self.pkl)
    self.assets.append(asset)

    # Loop on each Asset found.
    for aList in doc['PackingList']['AssetList']['Asset']:
      asset = {}
      asset['id'] = aList['Id']  # This must exist!
      asset['size'] = aList['Size']  # This must exist!
      if 'OriginalFileName' in aList:
        asset['file'] = aList['OriginalFileName']
      else:
        asset['file'] = 'None'
      self.assets.append(asset)

    return self.packageName

  # ------------------------------
  def initAssetMap(self):
    root = etree.Element('AssetMap')
    etree.SubElement(root, 'AnnotationText').text = 'DCP Manager Build ' + self.date
    etree.SubElement(root, 'VolumeCount').text = '1'
    etree.SubElement(root, 'IssueDate').text = self.time
    etree.SubElement(root, 'Issuer').text = 'Mark McFate'
    etree.SubElement(root, 'Creator').text = 'DCP Manager'
    # print etree.tostring(root, pretty_print=True, xml_declaration=True)
    xml = etree.ElementTree(root)
    xml.write(self.destAssetXML, pretty_print=True, xml_declaration=True)

  # -------------------------------
  # if __name__ == '__main__':
  def load_SMTP_standards(self):
    self.serveraddress = 'summitt.dweller@gmail.com'
    self.SMTPServer = 'smtp.gmail.com'
    self.SMTPPort = 587
    # Read from the .netrc file in your home directory
    secrets = netrc.netrc()
    auth = secrets.authenticators(self.SMTPServer)
    if auth:
      self.SMTPUser = auth[0]
      self.SMTPPassword = auth[2]

  # -------------------------------
  def parse_args(self):
    # Parse arguments
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("OPERATION", help="Specify the DCP Manager operation to perform.")
    parser.add_argument("-s", "--source", help="Specify the directory to perform OPERATION on. Default is " + self.SOURCE)
    parser.add_argument("-d", "--destination", help="Specify the destination directory of the OPERATION. Default is " + self.DESTINATION)
    parser.add_argument("-m", "--mail", help="eMail address where the log file will be sent. Default is " + self.MAIL)
    parser.add_argument("-l", "--logfile", help="Specify the logfile to record opeartion activity. Default is " + self.LOGFILE)
    parser.add_argument("-p", "--packages", help="Specify a pattern to match package names. Used with the DELETE operation")
    parser.add_argument("--debug", help="Toggle debug output ON. Default is OFF")
    parser.add_argument("--scope", help="Toggle SCOPE ON to operate on packages with '_S_' in the name. Default is OFF")
    parser.add_argument("--not_tlr", help="Toggle NOT_TLR ON to operate on pacakges without '_TLR' in the name. Default is OFF")
    args = parser.parse_args()

    # Define variables
    self.op = args.OPERATION
    if not args.source:
      self.source = self.SOURCE
    else:
      self.source = args.source
    if not args.destination:
      self.dest = self.DESTINATION
    else:
      self.dest = args.destination
    if not args.mail:
      self.mail = self.MAIL
    else:
      self.mail = args.mail
    if not args.logfile:
      self.logfile = self.LOGFILE
    else:
      self.logfile = args.logfile
    if not args.debug:
      self.debug = self.DEBUG
    else:
      self.debug = True
    if not args.scope:
      self.scope = self.SCOPE
    else:
      self.scope = True
    if not args.not_tlr:
      self.not_tlr= self.NOT_TLR
    else:
      self.not_tlr = True
    if args.packages:
      self.pkgPattern = args.packages
    else:
      self.pkgPattern = 'None'

    self.args = args
    self.logger = logging.getLogger("logger")
    self.now = datetime.datetime.now()
    self.date = self.now.strftime("%Y-%m-%dT")
    self.time = self.now.strftime("%Y-%m-%dT%H:%M:%S-08:00")

    # Declare the ASSETMAP file(s)
    self.destAssetMap = self.dest + "/ASSETMAP"
    self.destAssetXML = self.destAssetMap + ".xml"

    # Logging
    FORMAT = '%(asctime)-15s (%(levelname)s): %(message)s'
    if self.logfile and self.mail:
      if self.check_dir_exist(self.logfile):
        os.remove(self.logfile)
      if not self.debug:
        logging.basicConfig(filename=self.logfile, filemode='w', level=logging.INFO, format=FORMAT,
                            datefmt='%Y.%m.%d %H:%M:%S')
      else:
        logging.basicConfig(filename=self.logfile, filemode='w', level=logging.DEBUG, format=FORMAT,
                            datefmt='%Y.%m.%d %H:%M:%S')
      consoleHandler = logging.StreamHandler()
      console_format = logging.Formatter(FORMAT)
      consoleHandler.setFormatter(console_format)
      consoleHandler.setLevel(logging.DEBUG) if self.debug else consoleHandler.setLevel(logging.INFO)
      self.logger.addHandler(consoleHandler)

    # Verify source directory exists
    if not self.check_dir_exist(self.source):
      msg = "Source directory " + self.source + " does not exist.  Exiting..."
      self.logger.error(msg)
      if self.mail:
        self.send_mail(self.mail, self.logfile, 1, msg)
        exit(1)

    # Verify destination directory exists
    if not self.check_dir_exist(self.dest):
      msg = "Destination directory " + self.dest + " does not exist.  Exiting..."
      self.logger.error(msg)
      if self.mail:
        self.send_mail(self.mail, self.logfile, 1, msg)
        exit(1)

  # ---------------------------------
  # Function to return a string minus some prefix
  def remove_prefix(self, text, prefix):
    if text.startswith(prefix):
      return text[len(prefix):]
    return text

  # -------------------------------
  # Function to determine if a directory exists
  def check_dir_exist(self, os_dir):
    if os.path.exists(os_dir):
      self.logger.debug("{} exists.".format(os_dir))
      return True
    else:
      self.logger.warning("{} does not exist.".format(os_dir))
      return False

  # -------------------------------
  # Mail mit Log senden
  # Params: Recipient, Logfile, Returncode, Output
  def send_mail(self, recipient, logfile, return_value, output):
    self.load_SMTP_standards()
    if self.mail:
      if logfile:
        # Open the logfile for reading.
        fp = open(self.logfile, 'rb')
        # Create a text/plain message
        msg = MIMEText(fp.read())
        fp.close()
      elif output:
        msg = MIMEText(output)
      else:
        msg = MIMEText("The " + self.op + " operation is done.")
      if return_value == 0:
        msg['Subject'] = "The " + self.op + " operation has completed successfully."
      else:
        msg[
          'Subject'] = "The " + self.op + " operation did NOT succeed! An Error occured! Return Code = " + str(
          return_value) + "."
      msg['From'] = self.serveraddress
      msg['To'] = recipient

      try:
        self.conn = SMTP(self.SMTPServer, self.SMTPPort)
        self.conn.set_debuglevel(False)
        # Starting an encrypted TLS-Session
        self.conn.starttls()
        self.conn.login(self.SMTPUser, self.SMTPPassword)
        self.conn.sendmail(self.serveraddress, [recipient], msg.as_string())
        self.logger.info("Successfully sent mail to %s." % recipient)
      except Exception as e:
        self.logger.error("Sending mail failed! Error: %s." % e)
      finally:
        self.conn.close()


# -------------------------------
# Create and call the main()
x = dcp_manager();
x.main();
