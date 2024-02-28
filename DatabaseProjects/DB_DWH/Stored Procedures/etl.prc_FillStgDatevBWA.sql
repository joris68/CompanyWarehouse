CREATE PROCEDURE [etl].[prc_FillStgDatevBWA]
	@NameFile NVARCHAR(500)
AS

	BEGIN TRANSACTION; 
		BEGIN TRY 
		BEGIN 
		
			DELETE FROM [stg].[datevBWA] WHERE NameFile = @NameFile
	
			INSERT INTO [stg].[datevBWA]
			(
				[Zeile1]
				, [Bezeichnung]
				, [Konto]
				, [Vorzeichen]
				, [Zeile2]
				, [Zeilenbeschriftung]
				, [Wert]
				, [Wert2]
				, [Wert3]
				, [Wert4]
				, [FktSchl]
				, [NameFile]
			)
			SELECT 
				[Zeile1]
				, [Bezeichnung]
				, [Konto]
				, [Vorzeichen]
				, [Zeile2]
				, [Zeilenbeschriftung]
				, [Wert]
				, [Wert2]
				, [Wert3]
				, [Wert4]
				, [FktSchl]
				, @NameFile
			FROM [tmp].[datevBWA]
		END 

		IF @@TRANCOUNT > 0 
			COMMIT TRANSACTION; 
	END TRY 
	BEGIN CATCH 
		IF @@TRANCOUNT > 0 
			ROLLBACK TRANSACTION; 
		THROW; 
END CATCH; 
	
