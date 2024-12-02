from sqlalchemy.orm import Session
from datetime import datetime
import csv
from decimal import Decimal, InvalidOperation as decimal_InvalidOperation
import glob
import os
from .models import Flight, Aircraft, Account, Member, AccountEntry
from loguru import logger
from config import Config

class DataImporter:
    def import_members(self, session: Session, filename: str):
        """Import member records from a CSV file.
        
        Args:
            session: SQLAlchemy session
            filename: Path to CSV file
            
        Expected CSV columns:
        Jäsenen ID,Sukunimi,Etunimi,Sähköposti,Syntynyt,PIK-viite
        
        Returns:
            tuple: (number of imported records, number of skipped existing)
        """
        logger.debug(f"Importing members from {filename}")
        count = 0
        skipped = 0
        
        with open(filename, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            # Verify required columns
            required_columns = {'Sukunimi', 'Etunimi', 'PIK-viite'}
            missing_columns = required_columns - set(reader.fieldnames)
            if missing_columns:
                raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
                
            for row in reader:
                try:
                    # Check if member already exists
                    existing = session.query(Member).filter(
                        Member.id == row['PIK-viite']
                    ).first()
                    
                    if existing:
                        skipped += 1
                        continue
                    
                    # Parse birth date if provided
                    birth_date = None
                    if row.get('Syntynyt'):
                        try:
                            # Try Finnish format first (DD.MM.YYYY)
                            birth_date = datetime.strptime(row['Syntynyt'], '%d.%m.%Y').date()
                        except ValueError:
                            try:
                                # Fall back to ISO format (YYYY-MM-DD)
                                birth_date = datetime.strptime(row['Syntynyt'], '%Y-%m-%d').date()
                            except ValueError as e:
                                raise ValueError(f"Invalid birth date format: {row['Syntynyt']}. Use DD.MM.YYYY or YYYY-MM-DD")
                    
                    # Create new member
                    member = Member(
                        id=row['PIK-viite'],
                        name=f"{row['Etunimi']} {row['Sukunimi']}",
                        email=row.get('Sähköposti'),
                        birth_date=birth_date
                    )
                    session.add(member)
                    logger.info(f"Added new member: {member.name} (ID: {member.id}) {'with email ' + member.email if member.email else ''}")
                    count += 1
                    
                except Exception as e:
                    error_msg = f"Error in row {reader.line_num}: {str(e)}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                    
            return count, skipped

    def import_transactions(self, session: Session, filename: str):
        """Import transaction records from a CSV file.
        
        Args:
            session: SQLAlchemy session
            filename: Path to CSV file
            
        Expected CSV columns:
        Tapahtumapäivä,Maksajan viitenumero,Selite,Summa,nimi,kirjanpitovuosi,
        edellisten vuosien tapahtuma,Tili
        
        Returns:
            tuple: (number of imported records, number of skipped/failed)
        """
        logger.debug(f"Importing transactions from {filename}")
        count = 0
        failed = 0
        
        with open(filename, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            # Verify required columns
            required_columns = {'Tapahtumapäivä', 'Maksajan viitenumero', 'Selite', 'Summa', 'Tili'}
            missing_columns = required_columns - set(reader.fieldnames)
            if missing_columns:
                raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
                
            for row in reader:
                try:
                    # Parse date
                    try:
                        date = datetime.strptime(row['Tapahtumapäivä'], '%Y-%m-%d').date()
                    except ValueError:
                        raise ValueError(f"Invalid date format: {row['Tapahtumapäivä']}. Use YYYY-MM-DD")
                    
                    # Find account by reference number
                    account = session.query(Account).get(row['Maksajan viitenumero'])
                    if not account:
                        logger.warning(f"Account with reference ID {row['Maksajan viitenumero']} not found")
                        failed += 1
                        continue
                    
                    # Parse amount (assuming it's in decimal format)
                    try:
                        amount = Decimal(row['Summa'].replace(',', '.'))  # Handle both . and , as decimal separator
                    except (ValueError, decimal_InvalidOperation):
                        raise ValueError(f"Invalid amount format: {row['Summa']}")
                    
                    # Create new AccountEntry
                    entry = AccountEntry(
                        account_id=account.id,
                        date=date,
                        amount=amount,
                        description=row['Selite'],
                        ledger_account_id=row['Tili']
                    )
                    
                    session.add(entry)
                    count += 1
                    
                except Exception as e:
                    error_msg = f"Error in row {reader.line_num}: {str(e)}"
                    logger.error(error_msg)
                    failed += 1
                    continue
                    
            return count, failed

    def import_flights(self, session: Session, path_pattern: str):
        """Import flight records from CSV file(s).
        
        Args:
            session: SQLAlchemy session
            path_pattern: Path/pattern to CSV file(s), supports wildcards
            
        Expected CSV columns:
        Selite, Tapahtumapäivä, Maksajan viitenumero, Opettaja/Päällikkö,
        Oppilas/Matkustaja, Henkilöluku, Lähtöpaikka, Laskeutumispaikka,
        Lähtöaika, Laskeutumisaika, Lentoaika, Laskuja, Tarkoitus,
        Lentoaika_desimaalinen, Laskutuslisä syy, Pilveä
        
        Returns:
            tuple: (number of imported records, list of failed rows with error messages)
            
        Raises:
            ValueError: If any row fails to import, the entire transaction is rolled back
        """
        logger.debug(f"Importing flights from {path_pattern}")
        flights_to_add = []
        total_count = 0
        count = 0  # Initialize count
        failed_rows = []  # Initialize failed_rows at the start
        
        filename = path_pattern  # We now expect a single filename
        logger.debug(f"Importing flights from {filename}")
        with open(filename, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    # Extract registration number from Selite
                    selite_reg = row['Selite'].split()[0].upper()  # First word is registration

                    if selite_reg in Config.NO_INVOICING_AIRCRAFT:
                        logger.warning(f"Skipping flight for aircraft {selite_reg} (no-invoicing)")
                        continue
                    
                    # Find aircraft where registration contains the Selite number
                    aircraft = session.query(Aircraft).filter(
                        Aircraft.registration.contains(selite_reg)
                    ).first()
                    
                    if not aircraft:
                        raise ValueError(f"Aircraft {selite_reg} not found in database")

                    # Find account by reference number if not a no-invoice reference
                    reference_id = row['Maksajan viitenumero']
                    account = None
                    account_id = None
                    
                    if reference_id not in Config.NO_INVOICING_REFERENCE_IDS:
                        account = session.query(Account).get(reference_id)
                        if not account:
                            #raise ValueError(f"Account with reference ID {reference_id} not found")
                            logger.warning(f"Account with reference ID {reference_id} not found")
                            continue
                        account_id = account.id
                    
                    # Construct notes from available fields
                    notes_parts = []
                    if row.get('Opettaja/Päällikkö'):
                        notes_parts.append(f"Pilot: {row['Opettaja/Päällikkö']}")
                    if row.get('Oppilas/Matkustaja'):
                        notes_parts.append(f"Passenger: {row['Oppilas/Matkustaja']}")
                    if row.get('Tarkoitus'):
                        notes_parts.append(f"Purpose: {row['Tarkoitus']}")
                    if row.get('Laskutuslisä syy'):
                        notes_parts.append(f"Billing note: {row['Laskutuslisä syy']}")
                    
                    # Parse departure and landing times
                    date = datetime.strptime(row['Tapahtumapäivä'], '%Y-%m-%d')
                    
                    # Try parsing times with both : and . as separators
                    def parse_time(time_str, date_str):
                        try:
                            return datetime.strptime(f"{date_str} {time_str}", '%Y-%m-%d %H:%M')
                        except ValueError:
                            # Try with period instead of colon
                            return datetime.strptime(f"{date_str} {time_str.replace('.',':')}", '%Y-%m-%d %H:%M')
                            
                    departure_time = parse_time(row['Lähtöaika'], row['Tapahtumapäivä'])
                    landing_time = parse_time(row['Laskeutumisaika'], row['Tapahtumapäivä'])
                    
                    flight = Flight(
                        date=date,
                        departure_time=departure_time,
                        landing_time=landing_time,
                        reference_id=row['Maksajan viitenumero'],
                        aircraft_id=aircraft.id,
                        account_id=account_id,
                        duration=Decimal(row['Lentoaika_desimaalinen']),
                        notes='\n'.join(notes_parts) if notes_parts else None,
                        purpose=row.get('Tarkoitus') if row.get('Tarkoitus') else None
                    )
                    flights_to_add.append(flight)
                    count += 1
                    
                except Exception as e:
                    error_msg = f"Error in row {reader.line_num}: {str(e)}"
                    logger.error(error_msg)
                    failed_rows.append((row, error_msg))
                    continue

                total_count += 1

        if failed_rows:
            logger.warning(f"Failed to import {len(failed_rows)} rows")
            raise ValueError(f"Failed to import {len(failed_rows)} rows. No flights were imported.")
            
        # If we get here, all rows were valid - add them all
        for flight in flights_to_add:
            session.add(flight)
            
        return total_count, failed_rows
