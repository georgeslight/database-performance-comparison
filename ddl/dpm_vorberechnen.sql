CREATE FUNCTION dpm_vorberechnen (
	vin_id_t_import INTEGER,
	vin_daten_loeschen BOOLEAN,
	vin_with_bereiche BOOLEAN
) RETURNS INTEGER LANGUAGE plpgsql AS $$
begin
if vin_daten_loeschen then
	
	delete from <schemaName>.mv_dpm_vorberechnet_neu where id_t_import=vin_id_t_import;
	delete from <schemaName>.mv_dpm_vorberechnet_neu_uz where id_t_import=vin_id_t_import;
	if vin_with_bereiche then
    delete from <schemaName>.mv_dpm_vorberechnet_bereiche where id_t_import=vin_id_t_import;
end if;
	
end if;


insert into <schemaName>.mv_dpm_vorberechnet_neu (select * from <schemaName>.v_dpm_vorberechnet_neu where id_t_import=vin_id_t_import);
insert into <schemaName>.mv_dpm_vorberechnet_neu_uz (select * from <schemaName>.v_dpm_vorberechnet_neu_uz where id_t_import=vin_id_t_import);
if vin_with_bereiche then
    insert into <schemaName>.mv_dpm_vorberechnet_bereiche (select * from <schemaName>.v_dpm_vorberechnet_bereiche where id_t_import=vin_id_t_import);
end if;
update <schemaName>.import set vorberechnet=true where id_t_import=vin_id_t_import;

return vin_id_t_import;

end;
$$;