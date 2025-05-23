//
//import React, { useState, useEffect } from 'react';
//import axios from 'axios';
//import './App.css';
//
//function App() {
//  const [host, setHost] = useState('localhost');
//  const [port, setPort] = useState(8123);
//  const [database, setDatabase] = useState('uk_price_paid');
//  const [username, setUsername] = useState('ingestor_user');
//  const [password, setPassword] = useState('ingestor_pass');
//  const [jwtToken, setJwtToken] = useState('');
//  const [source, setSource] = useState('ClickHouse');
//  const [tableName, setTableName] = useState('');
//  const [columns, setColumns] = useState([]);
//  const [selectedColumns, setSelectedColumns] = useState([]);
//  const [fileName, setFileName] = useState('output.csv');
//  const [delimiter, setDelimiter] = useState(',');
//  const [status, setStatus] = useState('');
//  const [tables, setTables] = useState([]);
//
//  useEffect(() => {
//    fetchTables();
//  }, []);
//
//  const configureConnection = () => {
//    const details = { host, port, database, username, password, jwtToken };
//    axios.post('http://localhost:8080/api/ingestion/configure-connection', details)
//      .then(() => setStatus('Connection configured'))
//      .catch(error => setStatus('Error: ' + error.message));
//  };
//
//  const fetchTables = () => {
//    axios.get('http://localhost:8080/api/ingestion/tables')
//      .then(response => setTables(response.data))
//      .catch(error => setStatus('Error fetching tables: ' + error.message));
//  };
//
//  const handleTableChange = (e) => {
//    setTableName(e.target.value);
//    axios.get(`http://localhost:8080/api/ingestion/columns/${e.target.value}`)
//      .then(response => setColumns(response.data))
//      .catch(error => setStatus('Error fetching columns: ' + error.message));
//  };
//
//  const handleIngest = () => {
//    const request = { source, tableName, columns: selectedColumns, fileName, delimiter };
//    axios.post('http://localhost:8080/api/ingestion/ingest', request)
//      .then(response => setStatus(`Ingested ${response.data.recordCount} records: ${response.data.message}`))
//      .catch(error => setStatus('Error: ' + error.message));
//  };
//
//  return (
//    <div className="App">
//      <h1>ClickHouse & Flat File Ingestion Tool</h1>
//      <div>
//        <label>Host: </label><input value={host} onChange={e => setHost(e.target.value)} />
//        <label>Port: </label><input type="number" value={port} onChange={e => setPort(e.target.value)} />
//        <label>Database: </label><input value={database} onChange={e => setDatabase(e.target.value)} />
//        <label>Username: </label><input value={username} onChange={e => setUsername(e.target.value)} />
//        <label>Password: </label><input type="password" value={password} onChange={e => setPassword(e.target.value)} />
//        <label>JWT Token: </label><input value={jwtToken} onChange={e => setJwtToken(e.target.value)} />
//        <button onClick={configureConnection}>Configure</button>
//      </div>
//      <div>
//        <label>Source: </label>
//        <select value={source} onChange={e => setSource(e.target.value)}>
//          <option value="ClickHouse">ClickHouse</option>
//          <option value="FlatFile">Flat File</option>
//        </select>
//      </div>
//      {source === 'ClickHouse' && (
//        <div>
//          <select value={tableName} onChange={handleTableChange}>
//            <option value="">Select Table</option>
//            {tables.map(table => <option key={table} value={table}>{table}</option>)}
//          </select>
//          {columns.length > 0 && (
//            <div>
//              <h3>Select Columns:</h3>
//              {columns.map(column => (
//                <div key={column}>
//                  <input type="checkbox" value={column} onChange={e => {
//                    setSelectedColumns(prev => e.target.checked ? [...prev, column] : prev.filter(c => c !== column));
//                  }} />
//                  <label>{column}</label>
//                </div>
//              ))}
//            </div>
//          )}
//        </div>
//      )}
//      <div>
//        <label>File Name: </label><input value={fileName} onChange={e => setFileName(e.target.value)} />
//        <label>Delimiter: </label><input value={delimiter} onChange={e => setDelimiter(e.target.value)} />
//        <button onClick={handleIngest}>Ingest</button>
//      </div>
//      <div><h3>Status: {status}</h3></div>
//    </div>
//  );
//}
//
//export default App;
//
//
//
//
//
//
//
//














import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './App.css';

function App() {
  const [host, setHost] = useState('localhost');
  const [port, setPort] = useState(8123);
  const [database, setDatabase] = useState('uk_price_paid');
  const [username, setUsername] = useState('ingestor_user');
  const [password, setPassword] = useState('ingestor_pass');
  const [jwtToken, setJwtToken] = useState('');
  const [source, setSource] = useState('ClickHouse');
  const [tableName, setTableName] = useState('');
  const [columns, setColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [fileName, setFileName] = useState('output.csv');
  const [flatFileName, setFlatFileName] = useState('input.csv'); // For FlatFile source
  const [delimiter, setDelimiter] = useState(',');
  const [status, setStatus] = useState('');
  const [tables, setTables] = useState([]);
  const [data, setData] = useState([]); // For table data

  useEffect(() => {
    fetchTables();
  }, []);

  const configureConnection = () => {
    const details = { host, port, database, username, password, jwtToken };
    axios.post('http://localhost:8080/api/ingestion/configure-connection', details)
      .then(() => setStatus('Connection configured'))
      .catch(error => setStatus('Error: ' + error.message));
  };

  const fetchTables = () => {
    axios.get('http://localhost:8080/api/ingestion/tables')
      .then(response => setTables(response.data))
      .catch(error => setStatus('Error fetching tables: ' + error.message));
  };

  const fetchColumns = () => {
    if (source === 'ClickHouse' && tableName) {
      axios.get(`http://localhost:8080/api/ingestion/columns/${tableName}`)
        .then(response => setColumns(response.data))
        .catch(error => setStatus('Error fetching columns: ' + error.message));
    } else if (source === 'FlatFile' && flatFileName) {
      axios.get(`http://localhost:8080/api/ingestion/flatfile-columns?fileName=${flatFileName}&delimiter=${delimiter}`)
        .then(response => setColumns(response.data))
        .catch(error => setStatus('Error fetching flat file columns: ' + error.message));
    }
  };

  const fetchData = () => {
    axios.get('http://localhost:8080/api/ingestion/data', {
      params: {
        source,
        tableName: source === 'ClickHouse' ? tableName : undefined,
        fileName: source === 'ClickHouse' ? fileName : flatFileName,
        delimiter,
        columns: selectedColumns.length > 0 ? selectedColumns : undefined
      }
    })
      .then(response => setData(response.data))
      .catch(error => setStatus('Error fetching data: ' + error.message));
  };

  const handleTableChange = (e) => {
    setTableName(e.target.value);
    fetchColumns();
    fetchData();
  };

  const handleFlatFileChange = (e) => {
    setFlatFileName(e.target.value);
    fetchColumns();
    fetchData();
  };

  const handleIngest = () => {
    const request = {
      source,
      tableName: source === 'ClickHouse' ? tableName : undefined,
      fileName: source === 'ClickHouse' ? fileName : flatFileName,
      columns: selectedColumns,
      delimiter
    };
    axios.post('http://localhost:8080/api/ingestion/ingest', request)
      .then(response => {
        setStatus(`Ingested ${response.data.recordCount} records: ${response.data.message}`);
        fetchData(); // Refresh data after ingestion
      })
      .catch(error => setStatus('Error: ' + error.message));
  };

  return (
    <div className="App">
      <h1 className="title">ClickHouse & Flat File Ingestion Tool</h1>
      <div className="config-section">
        <label>Host: </label><input value={host} onChange={e => setHost(e.target.value)} className="input-field" />
        <label>Port: </label><input type="number" value={port} onChange={e => setPort(e.target.value)} className="input-field" />
        <label>Database: </label><input value={database} onChange={e => setDatabase(e.target.value)} className="input-field" />
        <label>Username: </label><input value={username} onChange={e => setUsername(e.target.value)} className="input-field" />
        <label>Password: </label><input type="password" value={password} onChange={e => setPassword(e.target.value)} className="input-field" />
        <label>JWT Token: </label><input value={jwtToken} onChange={e => setJwtToken(e.target.value)} className="input-field" />
        <button onClick={configureConnection} className="button">Configure</button>
      </div>
      <div className="source-section">
        <label>Source: </label>
        <select value={source} onChange={e => { setSource(e.target.value); setData([]); }} className="select-field">
          <option value="ClickHouse">ClickHouse</option>
          <option value="FlatFile">Flat File</option>
        </select>
      </div>
      {source === 'ClickHouse' && (
        <div className="table-section">
          <select value={tableName} onChange={handleTableChange} className="select-field">
            <option value="">Select Table</option>
            {tables.map(table => <option key={table} value={table}>{table}</option>)}
          </select>
          {columns.length > 0 && (
            <div className="columns-section">
              <h3>Select Columns:</h3>
              {columns.map(column => (
                <div key={column} className="checkbox-item">
                  <input type="checkbox" value={column} onChange={e => {
                    setSelectedColumns(prev => e.target.checked ? [...prev, column] : prev.filter(c => c !== column));
                  }} className="checkbox" />
                  <label>{column}</label>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
      {source === 'FlatFile' && (
        <div className="file-section">
          <label>File Name: </label><input value={flatFileName} onChange={handleFlatFileChange} className="input-field" />
          {columns.length > 0 && (
            <div className="columns-section">
              <h3>Select Columns:</h3>
              {columns.map(column => (
                <div key={column} className="checkbox-item">
                  <input type="checkbox" value={column} onChange={e => {
                    setSelectedColumns(prev => e.target.checked ? [...prev, column] : prev.filter(c => c !== column));
                  }} className="checkbox" />
                  <label>{column}</label>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
      <div className="action-section">
        <label>File Name: </label><input value={source === 'ClickHouse' ? fileName : flatFileName} onChange={e => {
          if (source === 'ClickHouse') setFileName(e.target.value);
          else setFlatFileName(e.target.value);
        }} className="input-field" />
        <label>Delimiter: </label><input value={delimiter} onChange={e => setDelimiter(e.target.value)} className="input-field" />
        <button onClick={handleIngest} className="button">Ingest</button>
        <button onClick={fetchData} className="button">Refresh Data</button>
      </div>
      <div className="status-section">
        <h3>Status: {status}</h3>
        {data.length > 0 && (
          <div className="table-container">
            <table className="data-table">
              <thead>
                <tr>
                  {columns.map(col => <th key={col}>{col}</th>)}
                </tr>
              </thead>
              <tbody>
                {data.map((row, index) => (
                  <tr key={index}>
                    {row.map((cell, i) => <td key={i}>{cell || 'N/A'}</td>)}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

export default App;